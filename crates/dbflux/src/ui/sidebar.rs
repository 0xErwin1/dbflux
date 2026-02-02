use crate::app::{AppState, AppStateChanged, ConnectedProfile};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use crate::ui::windows::connection_manager::ConnectionManagerWindow;
use crate::ui::windows::settings::SettingsWindow;
use dbflux_core::{
    CodeGenScope, ConnectionTreeNode, ConnectionTreeNodeKind, ConstraintKind, CustomTypeInfo,
    CustomTypeKind, DbKind, SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy,
    SchemaSnapshot, TableInfo, TableRef, TaskKind, ViewInfo,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Root;
use gpui_component::Sizable;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::list::ListItem;
use gpui_component::tree::{TreeItem, TreeState, tree};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub enum SidebarEvent {
    GenerateSql(String),
    RequestFocus,
    /// Request to open a table in a new DataDocument tab
    OpenTable {
        profile_id: Uuid,
        table: TableRef,
    },
    /// Request to show SQL preview modal
    RequestSqlPreview {
        profile_id: Uuid,
        table_info: TableInfo,
        generation_type: crate::ui::sql_preview_modal::SqlGenerationType,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeNodeKind {
    ConnectionFolder,
    Profile,
    Database,
    Schema,
    TablesFolder,
    ViewsFolder,
    TypesFolder,
    SchemaIndexesFolder,
    SchemaForeignKeysFolder,
    Table,
    View,
    CustomType,
    ColumnsFolder,
    IndexesFolder,
    ForeignKeysFolder,
    ConstraintsFolder,
    Column,
    Index,
    ForeignKey,
    SchemaIndex,
    SchemaForeignKey,
    Constraint,
    Unknown,
}

impl TreeNodeKind {
    fn from_id(id: &str) -> Self {
        match id {
            _ if id.starts_with("conn_folder_") => Self::ConnectionFolder,
            _ if id.starts_with("profile_") => Self::Profile,
            _ if id.starts_with("db_") => Self::Database,
            // schema_indexes_ and schema_fks_ must be checked before schema_
            _ if id.starts_with("schema_indexes_") => Self::SchemaIndexesFolder,
            _ if id.starts_with("schema_fks_") => Self::SchemaForeignKeysFolder,
            _ if id.starts_with("schema_") => Self::Schema,
            _ if id.starts_with("tables_") => Self::TablesFolder,
            _ if id.starts_with("views_") => Self::ViewsFolder,
            _ if id.starts_with("types_") => Self::TypesFolder,
            _ if id.starts_with("table_") => Self::Table,
            _ if id.starts_with("view_") => Self::View,
            _ if id.starts_with("customtype_") => Self::CustomType,
            _ if id.starts_with("columns_") => Self::ColumnsFolder,
            _ if id.starts_with("indexes_") => Self::IndexesFolder,
            _ if id.starts_with("fks_") => Self::ForeignKeysFolder,
            _ if id.starts_with("constraints_") => Self::ConstraintsFolder,
            _ if id.starts_with("col_") => Self::Column,
            _ if id.starts_with("sidx_") => Self::SchemaIndex,
            _ if id.starts_with("sfk_") => Self::SchemaForeignKey,
            _ if id.starts_with("idx_") => Self::Index,
            _ if id.starts_with("fk_") => Self::ForeignKey,
            _ if id.starts_with("constraint_") => Self::Constraint,
            _ => Self::Unknown,
        }
    }

    fn needs_click_handler(&self) -> bool {
        matches!(
            self,
            Self::Profile | Self::Database | Self::Table | Self::View | Self::ConnectionFolder
        )
    }

    fn shows_pointer_cursor(&self) -> bool {
        matches!(
            self,
            Self::Profile | Self::Database | Self::ConnectionFolder
        )
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
    // Folder actions
    NewFolder,
    NewConnection,
    RenameFolder,
    DeleteFolder,
    MoveToFolder(Option<Uuid>),
    // Schema object SQL generation
    GenerateIndexSql(IndexSqlAction),
    GenerateForeignKeySql(ForeignKeySqlAction),
    GenerateTypeSql(TypeSqlAction),
}

#[derive(Clone)]
pub enum IndexSqlAction {
    Create,
    Drop,
    Reindex,
}

#[derive(Clone)]
pub enum ForeignKeySqlAction {
    AddConstraint,
    DropConstraint,
}

#[derive(Clone)]
pub enum TypeSqlAction {
    Create,
    AddEnumValue,
    Drop,
}

impl ContextMenuAction {
    /// Returns the icon for this menu action
    fn icon(&self) -> Option<AppIcon> {
        match self {
            Self::Open => Some(AppIcon::Eye),
            Self::ViewSchema => Some(AppIcon::Table),
            Self::GenerateCode(_) => Some(AppIcon::Code),
            Self::Connect => Some(AppIcon::Plug),
            Self::Disconnect => Some(AppIcon::Unplug),
            Self::Edit => Some(AppIcon::Pencil),
            Self::Delete => Some(AppIcon::Delete),
            Self::OpenDatabase => Some(AppIcon::Database),
            Self::CloseDatabase => Some(AppIcon::Database),
            Self::Submenu(_) => None,
            Self::NewFolder => Some(AppIcon::Folder),
            Self::NewConnection => Some(AppIcon::Plug),
            Self::RenameFolder => Some(AppIcon::Pencil),
            Self::DeleteFolder => Some(AppIcon::Delete),
            Self::MoveToFolder(_) => Some(AppIcon::Folder),
            Self::GenerateIndexSql(_) => Some(AppIcon::Code),
            Self::GenerateForeignKeySql(_) => Some(AppIcon::Code),
            Self::GenerateTypeSql(_) => Some(AppIcon::Code),
        }
    }
}

#[derive(Clone)]
struct SidebarDragState {
    node_id: Uuid,
    additional_nodes: Vec<Uuid>,
    #[allow(dead_code)]
    is_folder: bool,
    label: String,
}

impl SidebarDragState {
    fn all_node_ids(&self) -> Vec<Uuid> {
        let mut ids = vec![self.node_id];
        ids.extend(self.additional_nodes.iter().copied());
        ids
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropPosition {
    #[allow(dead_code)]
    Before,
    Into,
    After,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DropTarget {
    item_id: String,
    position: DropPosition,
}

struct DragPreview {
    label: String,
}

impl Render for DragPreview {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        div()
            .bg(theme.sidebar)
            .border_1()
            .border_color(theme.drag_border)
            .rounded(Radii::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .text_size(FontSizes::SM)
            .text_color(theme.foreground)
            .shadow_md()
            .child(self.label.clone())
    }
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

/// Action to execute after table/type details finish loading.
#[derive(Clone)]
enum PendingAction {
    ViewSchema {
        item_id: String,
    },
    GenerateCode {
        item_id: String,
        generator_id: String,
    },
    ExpandTypesFolder {
        item_id: String,
    },
    ExpandSchemaIndexesFolder {
        item_id: String,
    },
    ExpandSchemaForeignKeysFolder {
        item_id: String,
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
    tree_state: Entity<TreeState>,
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
    /// ID currently being renamed (folder or profile)
    editing_id: Option<Uuid>,
    /// Type of item being renamed
    editing_is_folder: bool,
    /// Input state for rename
    rename_input: Entity<InputState>,
    /// Item ID pending rename (set by context menu, processed in render)
    pending_rename_item: Option<String>,
    /// Current drop target during drag operations
    drop_target: Option<DropTarget>,
    /// Folder being hovered during drag (for auto-expand)
    drag_hover_folder: Option<Uuid>,
    /// When the drag hover started (for delay before expand)
    drag_hover_start: Option<std::time::Instant>,
    /// Current auto-scroll direction during drag (-1 = up, 1 = down, 0 = none)
    auto_scroll_direction: i32,
    /// Multi-selected items (item IDs) for bulk operations
    multi_selection: HashSet<String>,
    /// Item ID pending delete confirmation (for keyboard x shortcut)
    pending_delete_item: Option<String>,
    /// Delete confirmation modal state (for context menu delete)
    delete_confirm_modal: Option<DeleteConfirmState>,
    /// Whether the add menu dropdown is open
    add_menu_open: bool,
}

struct PendingToast {
    message: String,
    is_error: bool,
}

struct DeleteConfirmState {
    item_id: String,
    item_name: String,
    is_folder: bool,
}

impl EventEmitter<SidebarEvent> for Sidebar {}

impl Sidebar {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let items = Self::build_tree_items(app_state.read(cx));
        let visible_entry_count = Self::count_visible_entries(&items);
        let tree_state = cx.new(|cx| TreeState::new(cx).items(items));

        let rename_input = cx.new(|cx| InputState::new(window, cx));

        let app_state_subscription = cx.subscribe(&app_state, |this, _app_state, _event, cx| {
            this.refresh_tree(cx);
        });

        let rename_subscription = cx.subscribe_in(
            &rename_input,
            window,
            |this, _, event: &InputEvent, _, cx| match event {
                InputEvent::PressEnter { .. } => {
                    this.commit_rename(cx);
                }
                InputEvent::Blur => {
                    this.cancel_rename(cx);
                }
                _ => {}
            },
        );

        Self {
            app_state,
            tree_state,
            pending_toast: None,
            connections_focused: false,
            visible_entry_count,
            expansion_overrides: HashMap::new(),
            context_menu: None,
            pending_action: None,
            active_databases: HashMap::new(),
            _subscriptions: vec![app_state_subscription, rename_subscription],
            editing_id: None,
            editing_is_folder: false,
            rename_input,
            pending_rename_item: None,
            drop_target: None,
            drag_hover_folder: None,
            drag_hover_start: None,
            auto_scroll_direction: 0,
            multi_selection: HashSet::new(),
            pending_delete_item: None,
            delete_confirm_modal: None,
            add_menu_open: false,
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

        self.pending_delete_item = None;

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

        self.pending_delete_item = None;

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

    pub fn extend_select_next(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        // Add current item to selection
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }

        let current = self.tree_state.read(cx).selected_index();
        let next = match current {
            Some(idx) => (idx + 1).min(self.visible_entry_count.saturating_sub(1)),
            None => 0,
        };

        // Move to next and add it to selection
        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(next), cx);
            state.scroll_to_item(next, gpui::ScrollStrategy::Center);
        });

        // Add the new item to selection
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }

        cx.notify();
    }

    pub fn extend_select_prev(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        // Add current item to selection
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }

        let current = self.tree_state.read(cx).selected_index();
        let prev = match current {
            Some(idx) => idx.saturating_sub(1),
            None => self.visible_entry_count.saturating_sub(1),
        };

        // Move to prev and add it to selection
        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(prev), cx);
            state.scroll_to_item(prev, gpui::ScrollStrategy::Center);
        });

        // Add the new item to selection
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }

        cx.notify();
    }

    pub fn toggle_current_selection(&mut self, cx: &mut Context<Self>) {
        let entry = self.tree_state.read(cx).selected_entry().cloned();
        if let Some(entry) = entry {
            let item_id = entry.item().id.to_string();
            self.toggle_selection(&item_id, cx);
        }
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

        // When expanding a Data Types folder, check if types need to be loaded
        if expanded
            && item_id.starts_with("types_")
            && let Some((profile_id, database, schema)) =
                Self::parse_schema_folder_id(item_id, "types_")
        {
            let needs_fetch =
                self.app_state
                    .read(cx)
                    .needs_schema_types(profile_id, &database, Some(&schema));

            if needs_fetch {
                let pending = PendingAction::ExpandTypesFolder {
                    item_id: item_id.to_string(),
                };
                self.spawn_fetch_schema_types(profile_id, &database, Some(&schema), pending, cx);
                return;
            }
        }

        // When expanding schema-level Indexes folder, check if indexes need to be loaded
        if expanded
            && item_id.starts_with("schema_indexes_")
            && let Some((profile_id, database, schema)) =
                Self::parse_schema_folder_id(item_id, "schema_indexes_")
        {
            let needs_fetch =
                self.app_state
                    .read(cx)
                    .needs_schema_indexes(profile_id, &database, Some(&schema));

            if needs_fetch {
                let pending = PendingAction::ExpandSchemaIndexesFolder {
                    item_id: item_id.to_string(),
                };
                self.spawn_fetch_schema_indexes(profile_id, &database, Some(&schema), pending, cx);
                return;
            }
        }

        // When expanding schema-level Foreign Keys folder, check if FKs need to be loaded
        if expanded
            && item_id.starts_with("schema_fks_")
            && let Some((profile_id, database, schema)) =
                Self::parse_schema_folder_id(item_id, "schema_fks_")
        {
            let needs_fetch = self.app_state.read(cx).needs_schema_foreign_keys(
                profile_id,
                &database,
                Some(&schema),
            );

            if needs_fetch {
                let pending = PendingAction::ExpandSchemaForeignKeysFolder {
                    item_id: item_id.to_string(),
                };
                self.spawn_fetch_schema_foreign_keys(
                    profile_id,
                    &database,
                    Some(&schema),
                    pending,
                    cx,
                );
                return;
            }
        }

        // Sync folder collapsed state with AppState
        if item_id.starts_with("conn_folder_")
            && let Some(folder_id_str) = item_id.strip_prefix("conn_folder_")
            && let Ok(folder_id) = Uuid::parse_str(folder_id_str)
        {
            self.app_state.update(cx, |state, _cx| {
                state.set_folder_collapsed(folder_id, !expanded);
            });
        }

        self.expansion_overrides
            .insert(item_id.to_string(), expanded);
        self.rebuild_tree_with_overrides(cx);
    }

    /// Parse a schema-level folder ID to extract (profile_id, database, schema).
    ///
    /// Format: `{prefix}{uuid}_{database}_{schema}` where prefix is "types_", "schema_indexes_", etc.
    fn parse_schema_folder_id(item_id: &str, prefix: &str) -> Option<(Uuid, String, String)> {
        let rest = item_id.strip_prefix(prefix)?;

        // UUID is 36 chars, followed by "_"
        if rest.len() < 37 {
            return None;
        }

        let uuid_str = rest.get(..36)?;
        let profile_id = Uuid::parse_str(uuid_str).ok()?;

        // After UUID, we have _{database}_{schema}
        let remainder = rest.get(37..)?.to_string();
        if remainder.is_empty() {
            return None;
        }

        // remainder is "database_schema"
        if let Some(underscore_pos) = remainder.find('_') {
            let database = remainder[..underscore_pos].to_string();
            let schema = remainder[underscore_pos + 1..].to_string();
            if !database.is_empty() && !schema.is_empty() {
                return Some((profile_id, database, schema));
            }
        }

        None
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
            TreeNodeKind::ConnectionFolder => {
                self.toggle_item_expansion(item_id, cx);
            }
            _ => {}
        }
    }

    fn handle_item_click(
        &mut self,
        item_id: &str,
        click_count: usize,
        with_ctrl: bool,
        cx: &mut Context<Self>,
    ) {
        cx.emit(SidebarEvent::RequestFocus);

        // Ctrl+Click: toggle item in multi-selection
        if with_ctrl && click_count == 1 {
            self.toggle_selection(item_id, cx);
            // Also update tree selection to the clicked item
            if let Some(idx) = self.find_item_index(item_id, cx) {
                self.tree_state.update(cx, |state, cx| {
                    state.set_selected_index(Some(idx), cx);
                });
            }
            cx.notify();
            return;
        }

        // Normal click: clear multi-selection and select single item
        self.clear_selection(cx);

        if let Some(idx) = self.find_item_index(item_id, cx) {
            self.tree_state.update(cx, |state, cx| {
                state.set_selected_index(Some(idx), cx);
            });
        }

        if click_count == 2 {
            self.execute_item(item_id, cx);
        } else {
            let node_kind = TreeNodeKind::from_id(item_id);
            if matches!(
                node_kind,
                TreeNodeKind::Profile | TreeNodeKind::Database | TreeNodeKind::ConnectionFolder
            ) {
                self.toggle_item_expansion(item_id, cx);
            }
        }

        cx.notify();
    }

    fn browse_table(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if let Some(parts) = Self::parse_table_or_view_id(item_id) {
            let table = TableRef::with_schema(&parts.schema_name, &parts.object_name);
            cx.emit(SidebarEvent::OpenTable {
                profile_id: parts.profile_id,
                table,
            });
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
            .and_then(|db_schema| db_schema.views.iter().find(|v| v.name == parts.object_name));

        // Fall back to schema.schemas (PostgreSQL/SQLite)
        let view = view_from_db_schemas.or_else(|| Self::find_view_for_item(&parts, &conn.schema));

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
            foreign_keys: None,
            constraints: None,
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
        use crate::ui::sql_preview_modal::SqlGenerationType;

        let Some(parts) = Self::parse_table_or_view_id(item_id) else {
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&parts.profile_id) else {
            return;
        };

        // Try to convert to SqlGenerationType for preview modal
        let generation_type = SqlGenerationType::from_generator_id(generator_id);

        // First check the table_details cache (populated by ensure_table_details)
        let cache_key = (parts.schema_name.clone(), parts.object_name.clone());
        if let Some(table) = conn.table_details.get(&cache_key) {
            // For supported types, use the SQL preview modal
            if let Some(gen_type) = generation_type {
                cx.emit(SidebarEvent::RequestSqlPreview {
                    profile_id: parts.profile_id,
                    table_info: table.clone(),
                    generation_type: gen_type,
                });
                return;
            }

            // For unsupported types (CREATE TABLE, DROP, TRUNCATE), use driver generation
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
        let table_from_db_schemas =
            conn.database_schemas
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

        // For supported types, use the SQL preview modal
        if let Some(gen_type) = generation_type {
            cx.emit(SidebarEvent::RequestSqlPreview {
                profile_id: parts.profile_id,
                table_info: table.clone(),
                generation_type: gen_type,
            });
            return;
        }

        // For unsupported types, use driver generation
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

    fn get_current_database(conn: &ConnectedProfile) -> String {
        conn.active_database
            .clone()
            .or_else(|| {
                conn.schema
                    .as_ref()
                    .and_then(|s| s.current_database.clone())
            })
            .unwrap_or_else(|| "main".to_string())
    }

    fn get_db_kind_for_item(&self, item_id: &str, cx: &App) -> Option<DbKind> {
        let profile_id = Self::extract_profile_id_from_item(item_id)?;
        let state = self.app_state.read(cx);
        let conn = state.connections.get(&profile_id)?;
        Some(conn.connection.kind())
    }

    fn extract_profile_id_from_item(item_id: &str) -> Option<Uuid> {
        // Try various prefixes used in item IDs
        let prefixes = [
            "idx_",
            "sidx_",
            "fk_",
            "sfk_",
            "customtype_",
            "table_",
            "view_",
        ];

        for prefix in prefixes {
            if let Some(rest) = item_id.strip_prefix(prefix)
                && rest.len() >= 36
                && let Ok(uuid) = Uuid::parse_str(&rest[..36])
            {
                return Some(uuid);
            }
        }

        None
    }

    fn is_enum_type(&self, item_id: &str, cx: &App) -> bool {
        let Some((profile_id, schema_name, type_name)) = Self::parse_custom_type_id(item_id) else {
            return false;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&profile_id) else {
            return false;
        };

        let current_db = Self::get_current_database(conn);
        let cache_key = format!("{}__{}", current_db, schema_name);
        if let Some(types) = conn.schema_types.get(&cache_key) {
            return types
                .iter()
                .any(|t| t.name == type_name && t.kind == CustomTypeKind::Enum);
        }

        false
    }

    fn parse_custom_type_id(item_id: &str) -> Option<(Uuid, String, String)> {
        let rest = item_id.strip_prefix("customtype_")?;
        if rest.len() < 36 {
            return None;
        }

        let profile_id = Uuid::parse_str(&rest[..36]).ok()?;
        let remainder = rest.get(38..)?;

        let mut parts = remainder.splitn(2, "__");
        let schema_name = parts.next()?.to_string();
        let type_name = parts.next()?.to_string();

        if type_name.is_empty() {
            return None;
        }

        Some((profile_id, schema_name, type_name))
    }

    fn parse_index_id(item_id: &str) -> Option<(Uuid, String, String, bool)> {
        // Table-level: idx_{profile_id}__{table_name}__{index_name}
        if let Some(rest) = item_id.strip_prefix("idx_") {
            if rest.len() < 36 {
                return None;
            }

            let profile_id = Uuid::parse_str(&rest[..36]).ok()?;
            let remainder = rest.get(38..)?;

            let mut parts = remainder.splitn(2, "__");
            let table_name = parts.next()?.to_string();
            let index_name = parts.next()?.to_string();

            if index_name.is_empty() {
                return None;
            }

            return Some((profile_id, table_name, index_name, false));
        }

        // Schema-level: sidx_{profile_id}__{schema_name}__{index_name}
        if let Some(rest) = item_id.strip_prefix("sidx_") {
            if rest.len() < 36 {
                return None;
            }

            let profile_id = Uuid::parse_str(&rest[..36]).ok()?;
            let remainder = rest.get(38..)?;

            let mut parts = remainder.splitn(2, "__");
            let schema_name = parts.next()?.to_string();
            let index_name = parts.next()?.to_string();

            if index_name.is_empty() {
                return None;
            }

            return Some((profile_id, schema_name, index_name, true));
        }

        None
    }

    fn parse_foreign_key_id(item_id: &str) -> Option<(Uuid, String, String, bool)> {
        // Table-level: fk_{profile_id}__{table_name}__{fk_name}
        if let Some(rest) = item_id.strip_prefix("fk_") {
            if rest.len() < 36 {
                return None;
            }

            let profile_id = Uuid::parse_str(&rest[..36]).ok()?;
            let remainder = rest.get(38..)?;

            let mut parts = remainder.splitn(2, "__");
            let table_name = parts.next()?.to_string();
            let fk_name = parts.next()?.to_string();

            if fk_name.is_empty() {
                return None;
            }

            return Some((profile_id, table_name, fk_name, false));
        }

        // Schema-level: sfk_{profile_id}__{schema_name}__{fk_name}
        if let Some(rest) = item_id.strip_prefix("sfk_") {
            if rest.len() < 36 {
                return None;
            }

            let profile_id = Uuid::parse_str(&rest[..36]).ok()?;
            let remainder = rest.get(38..)?;

            let mut parts = remainder.splitn(2, "__");
            let schema_name = parts.next()?.to_string();
            let fk_name = parts.next()?.to_string();

            if fk_name.is_empty() {
                return None;
            }

            return Some((profile_id, schema_name, fk_name, true));
        }

        None
    }

    fn generate_index_sql(
        &mut self,
        item_id: &str,
        action: IndexSqlAction,
        cx: &mut Context<Self>,
    ) {
        let Some((profile_id, context_name, index_name, is_schema_level)) =
            Self::parse_index_id(item_id)
        else {
            log::warn!("Failed to parse index id: {}", item_id);
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&profile_id) else {
            return;
        };

        let db_kind = conn.connection.kind();
        let current_db = Self::get_current_database(conn);

        // Find the index info
        let index_info = if is_schema_level {
            let cache_key = format!("{}__{}", current_db, context_name);
            conn.schema_indexes.get(&cache_key).and_then(|indexes| {
                indexes
                    .iter()
                    .find(|idx| idx.name == index_name)
                    .map(|idx| (idx.table_name.clone(), idx.columns.clone(), idx.is_unique))
            })
        } else {
            // For table-level, find in table_details
            let table_name = context_name.clone();
            conn.table_details
                .values()
                .find(|t| t.name == table_name)
                .and_then(|t| t.indexes.as_ref())
                .and_then(|indexes| {
                    indexes
                        .iter()
                        .find(|idx| idx.name == index_name)
                        .map(|idx| (table_name.clone(), idx.columns.clone(), idx.is_unique))
                })
        };

        let sql = match action {
            IndexSqlAction::Create => {
                if let Some((table_name, columns, is_unique)) = index_info {
                    let unique = if is_unique { "UNIQUE " } else { "" };
                    let cols = columns.join(", ");

                    match db_kind {
                        DbKind::Postgres => format!(
                            "CREATE {}INDEX {} ON {}.{} ({});",
                            unique, index_name, context_name, table_name, cols
                        ),
                        DbKind::MySQL | DbKind::MariaDB => format!(
                            "CREATE {}INDEX {} ON {}.{} ({});",
                            unique, index_name, current_db, table_name, cols
                        ),
                        DbKind::SQLite => format!(
                            "CREATE {}INDEX {} ON {} ({});",
                            unique, index_name, table_name, cols
                        ),
                    }
                } else {
                    match db_kind {
                        DbKind::Postgres => format!(
                            "CREATE INDEX {} ON schema.table_name (column1, column2);",
                            index_name
                        ),
                        DbKind::MySQL | DbKind::MariaDB => format!(
                            "CREATE INDEX {} ON database.table_name (column1, column2);",
                            index_name
                        ),
                        DbKind::SQLite => format!(
                            "CREATE INDEX {} ON table_name (column1, column2);",
                            index_name
                        ),
                    }
                }
            }

            IndexSqlAction::Drop => match db_kind {
                DbKind::Postgres => format!("DROP INDEX {}.{};", context_name, index_name),
                DbKind::MySQL | DbKind::MariaDB => {
                    if let Some((table_name, _, _)) = index_info {
                        format!(
                            "DROP INDEX {} ON {}.{};",
                            index_name, current_db, table_name
                        )
                    } else {
                        format!("DROP INDEX {} ON {}.table_name;", index_name, current_db)
                    }
                }
                DbKind::SQLite => format!("DROP INDEX {};", index_name),
            },

            // REINDEX is filtered to only PostgreSQL and SQLite in build_context_menu_items
            IndexSqlAction::Reindex => match db_kind {
                DbKind::Postgres => format!("REINDEX INDEX {}.{};", context_name, index_name),
                DbKind::SQLite => format!("REINDEX {};", index_name),
                DbKind::MySQL | DbKind::MariaDB => {
                    // This branch shouldn't be reached due to menu filtering, but handle gracefully
                    format!("-- REINDEX not supported; index: {}", index_name)
                }
            },
        };

        cx.emit(SidebarEvent::GenerateSql(sql));
    }

    fn generate_foreign_key_sql(
        &mut self,
        item_id: &str,
        action: ForeignKeySqlAction,
        cx: &mut Context<Self>,
    ) {
        let Some((profile_id, context_name, fk_name, is_schema_level)) =
            Self::parse_foreign_key_id(item_id)
        else {
            log::warn!("Failed to parse foreign key id: {}", item_id);
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&profile_id) else {
            return;
        };

        let db_kind = conn.connection.kind();
        let current_db = Self::get_current_database(conn);

        // Find the FK info
        let fk_info = if is_schema_level {
            let cache_key = format!("{}__{}", current_db, context_name);
            conn.schema_foreign_keys.get(&cache_key).and_then(|fks| {
                fks.iter().find(|fk| fk.name == fk_name).map(|fk| {
                    (
                        fk.table_name.clone(),
                        fk.columns.clone(),
                        fk.referenced_schema.clone(),
                        fk.referenced_table.clone(),
                        fk.referenced_columns.clone(),
                        fk.on_delete.clone(),
                        fk.on_update.clone(),
                    )
                })
            })
        } else {
            // For table-level, find in table_details
            let table_name = context_name.clone();
            conn.table_details
                .values()
                .find(|t| t.name == table_name)
                .and_then(|t| t.foreign_keys.as_ref())
                .and_then(|fks| {
                    fks.iter().find(|fk| fk.name == fk_name).map(|fk| {
                        (
                            table_name.clone(),
                            fk.columns.clone(),
                            fk.referenced_schema.clone(),
                            fk.referenced_table.clone(),
                            fk.referenced_columns.clone(),
                            fk.on_delete.clone(),
                            fk.on_update.clone(),
                        )
                    })
                })
        };

        // SQLite is filtered out in build_context_menu_items (doesn't support ALTER TABLE for FKs)
        let sql = match action {
            ForeignKeySqlAction::AddConstraint => {
                if let Some((
                    table_name,
                    columns,
                    ref_schema,
                    ref_table,
                    ref_columns,
                    on_delete,
                    on_update,
                )) = fk_info
                {
                    let cols = columns.join(", ");
                    let ref_cols = ref_columns.join(", ");

                    let ref_table_full = if let Some(schema) = ref_schema {
                        format!("{}.{}", schema, ref_table)
                    } else {
                        ref_table
                    };

                    let on_delete_clause = on_delete
                        .map(|d| format!(" ON DELETE {}", d))
                        .unwrap_or_default();
                    let on_update_clause = on_update
                        .map(|u| format!(" ON UPDATE {}", u))
                        .unwrap_or_default();

                    match db_kind {
                        DbKind::Postgres => format!(
                            "ALTER TABLE {}.{}\n    ADD CONSTRAINT {}\n    FOREIGN KEY ({})\n    REFERENCES {} ({}){}{};",
                            context_name,
                            table_name,
                            fk_name,
                            cols,
                            ref_table_full,
                            ref_cols,
                            on_delete_clause,
                            on_update_clause
                        ),
                        DbKind::MySQL | DbKind::MariaDB | DbKind::SQLite => format!(
                            "ALTER TABLE {}.{}\n    ADD CONSTRAINT {}\n    FOREIGN KEY ({})\n    REFERENCES {} ({}){}{};",
                            current_db,
                            table_name,
                            fk_name,
                            cols,
                            ref_table_full,
                            ref_cols,
                            on_delete_clause,
                            on_update_clause
                        ),
                    }
                } else {
                    match db_kind {
                        DbKind::Postgres => format!(
                            "ALTER TABLE schema.table_name\n    ADD CONSTRAINT {}\n    FOREIGN KEY (column_name)\n    REFERENCES ref_table (ref_column);",
                            fk_name
                        ),
                        DbKind::MySQL | DbKind::MariaDB | DbKind::SQLite => format!(
                            "ALTER TABLE database.table_name\n    ADD CONSTRAINT {}\n    FOREIGN KEY (column_name)\n    REFERENCES ref_table (ref_column);",
                            fk_name
                        ),
                    }
                }
            }

            ForeignKeySqlAction::DropConstraint => {
                if let Some((table_name, ..)) = fk_info {
                    match db_kind {
                        DbKind::Postgres => format!(
                            "ALTER TABLE {}.{} DROP CONSTRAINT {};",
                            context_name, table_name, fk_name
                        ),
                        DbKind::MySQL | DbKind::MariaDB | DbKind::SQLite => format!(
                            "ALTER TABLE {}.{} DROP FOREIGN KEY {};",
                            current_db, table_name, fk_name
                        ),
                    }
                } else {
                    match db_kind {
                        DbKind::Postgres => {
                            format!("ALTER TABLE schema.table_name DROP CONSTRAINT {};", fk_name)
                        }
                        DbKind::MySQL | DbKind::MariaDB | DbKind::SQLite => format!(
                            "ALTER TABLE database.table_name DROP FOREIGN KEY {};",
                            fk_name
                        ),
                    }
                }
            }
        };

        cx.emit(SidebarEvent::GenerateSql(sql));
    }

    fn generate_type_sql(&mut self, item_id: &str, action: TypeSqlAction, cx: &mut Context<Self>) {
        let Some((profile_id, schema_name, type_name)) = Self::parse_custom_type_id(item_id) else {
            log::warn!("Failed to parse custom type id: {}", item_id);
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections.get(&profile_id) else {
            return;
        };

        let db_kind = conn.connection.kind();
        let current_db = Self::get_current_database(conn);

        // Find the type info
        let cache_key = format!("{}__{}", current_db, schema_name);
        let type_info = conn
            .schema_types
            .get(&cache_key)
            .and_then(|types| types.iter().find(|t| t.name == type_name));

        // Only PostgreSQL supports custom types (filtered in build_context_menu_items)
        let sql = match action {
            TypeSqlAction::Create => {
                if let Some(type_info) = type_info {
                    match type_info.kind {
                        CustomTypeKind::Enum => {
                            let values = type_info
                                .enum_values
                                .as_ref()
                                .map(|v| {
                                    v.iter()
                                        .map(|s| format!("'{}'", s))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                })
                                .unwrap_or_else(|| "'value1', 'value2'".to_string());

                            format!(
                                "CREATE TYPE {}.{} AS ENUM ({});",
                                schema_name, type_name, values
                            )
                        }
                        CustomTypeKind::Domain => {
                            let base_type = type_info
                                .base_type
                                .as_ref()
                                .cloned()
                                .unwrap_or_else(|| "text".to_string());

                            format!(
                                "CREATE DOMAIN {}.{} AS {};",
                                schema_name, type_name, base_type
                            )
                        }
                        CustomTypeKind::Composite => {
                            format!(
                                "CREATE TYPE {}.{} AS (\n    field1 type1,\n    field2 type2\n);",
                                schema_name, type_name
                            )
                        }
                    }
                } else {
                    format!(
                        "CREATE TYPE {}.{} AS ENUM ('value1', 'value2');",
                        schema_name, type_name
                    )
                }
            }

            TypeSqlAction::AddEnumValue => {
                format!(
                    "ALTER TYPE {}.{} ADD VALUE 'new_value';",
                    schema_name, type_name
                )
            }

            TypeSqlAction::Drop => format!("DROP TYPE {}.{};", schema_name, type_name),
        };

        let _ = db_kind;

        cx.emit(SidebarEvent::GenerateSql(sql));
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
                return db_schema.views.iter().find(|v| v.name == parts.object_name);
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
            && let Some(table) = db_schema
                .tables
                .iter()
                .find(|t| t.name == parts.object_name)
            && table.columns.is_some()
        {
            return TableDetailsStatus::Ready;
        }

        // Check schema.schemas (PostgreSQL/SQLite path)
        if let Some(ref schema) = conn.schema {
            for db_schema in &schema.schemas {
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

    /// Spawn a background task to fetch custom types for a schema.
    fn spawn_fetch_schema_types(
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
    fn spawn_fetch_schema_indexes(
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
    fn spawn_fetch_schema_foreign_keys(
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
            PendingAction::ExpandTypesFolder { item_id }
            | PendingAction::ExpandSchemaIndexesFolder { item_id }
            | PendingAction::ExpandSchemaForeignKeysFolder { item_id } => {
                self.expand_schema_folder(&item_id, cx);
            }
        }
    }

    fn expand_schema_folder(&mut self, item_id: &str, cx: &mut Context<Self>) {
        self.expansion_overrides.insert(item_id.to_string(), true);
        self.rebuild_tree_with_overrides(cx);
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
            TreeNodeKind::ConnectionFolder => {
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

            TreeNodeKind::Index | TreeNodeKind::SchemaIndex => {
                let db_kind = self.get_db_kind_for_item(item_id, cx);

                let mut submenu = vec![
                    ContextMenuItem {
                        label: "CREATE INDEX".into(),
                        action: ContextMenuAction::GenerateIndexSql(IndexSqlAction::Create),
                    },
                    ContextMenuItem {
                        label: "DROP INDEX".into(),
                        action: ContextMenuAction::GenerateIndexSql(IndexSqlAction::Drop),
                    },
                ];

                // REINDEX is supported by PostgreSQL and SQLite, not MySQL/MariaDB
                if matches!(db_kind, Some(DbKind::Postgres) | Some(DbKind::SQLite)) {
                    submenu.push(ContextMenuItem {
                        label: "REINDEX".into(),
                        action: ContextMenuAction::GenerateIndexSql(IndexSqlAction::Reindex),
                    });
                }

                vec![ContextMenuItem {
                    label: "Generate SQL".into(),
                    action: ContextMenuAction::Submenu(submenu),
                }]
            }

            TreeNodeKind::ForeignKey | TreeNodeKind::SchemaForeignKey => {
                let db_kind = self.get_db_kind_for_item(item_id, cx);

                // SQLite doesn't support ALTER TABLE ADD/DROP CONSTRAINT for FKs
                if matches!(db_kind, Some(DbKind::SQLite)) {
                    return vec![];
                }

                vec![ContextMenuItem {
                    label: "Generate SQL".into(),
                    action: ContextMenuAction::Submenu(vec![
                        ContextMenuItem {
                            label: "ADD CONSTRAINT".into(),
                            action: ContextMenuAction::GenerateForeignKeySql(
                                ForeignKeySqlAction::AddConstraint,
                            ),
                        },
                        ContextMenuItem {
                            label: "DROP CONSTRAINT".into(),
                            action: ContextMenuAction::GenerateForeignKeySql(
                                ForeignKeySqlAction::DropConstraint,
                            ),
                        },
                    ]),
                }]
            }

            TreeNodeKind::CustomType => {
                let db_kind = self.get_db_kind_for_item(item_id, cx);

                // Only PostgreSQL supports custom types
                if !matches!(db_kind, Some(DbKind::Postgres)) {
                    return vec![];
                }

                let mut submenu = vec![
                    ContextMenuItem {
                        label: "CREATE TYPE".into(),
                        action: ContextMenuAction::GenerateTypeSql(TypeSqlAction::Create),
                    },
                    ContextMenuItem {
                        label: "DROP TYPE".into(),
                        action: ContextMenuAction::GenerateTypeSql(TypeSqlAction::Drop),
                    },
                ];

                // Check if this is an enum type to show ADD VALUE option
                if self.is_enum_type(item_id, cx) {
                    submenu.insert(
                        1,
                        ContextMenuItem {
                            label: "ADD VALUE".into(),
                            action: ContextMenuAction::GenerateTypeSql(TypeSqlAction::AddEnumValue),
                        },
                    );
                }

                vec![ContextMenuItem {
                    label: "Generate SQL".into(),
                    action: ContextMenuAction::Submenu(submenu),
                }]
            }

            _ => vec![],
        }
    }

    /// Builds the "Move to..." submenu items for a profile or folder.
    fn build_move_to_submenu(&self, item_id: &str, cx: &App) -> Vec<ContextMenuItem> {
        let state = self.app_state.read(cx);
        let mut items = Vec::new();

        // Determine current node info (works for both profiles and folders)
        let (current_parent, current_node_id) = if let Some(profile_id_str) =
            item_id.strip_prefix("profile_")
            && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
        {
            let node = state.connection_tree.find_by_profile(profile_id);
            (node.and_then(|n| n.parent_id), node.map(|n| n.id))
        } else if let Some(folder_id_str) = item_id.strip_prefix("conn_folder_")
            && let Ok(folder_id) = Uuid::parse_str(folder_id_str)
        {
            let node = state.connection_tree.find_by_id(folder_id);
            (node.and_then(|n| n.parent_id), Some(folder_id))
        } else {
            (None, None)
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
            .map(|id| state.connection_tree.get_descendants(id))
            .unwrap_or_default();

        for folder in state.connection_tree.folders() {
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
                self.show_delete_confirm_modal(&item_id, cx);
            }
            ContextMenuAction::OpenDatabase => {
                self.handle_database_click(&item_id, cx);
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

    pub fn has_multi_selection(&self) -> bool {
        !self.multi_selection.is_empty()
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

    /// Creates a new folder at the root level.
    pub fn create_root_folder(&mut self, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.create_folder("New Folder", None);
            cx.emit(AppStateChanged);
        });

        self.refresh_tree(cx);
    }

    fn create_folder_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        // Determine parent folder ID from item_id
        let parent_id = if let Some(folder_id_str) = item_id.strip_prefix("conn_folder_") {
            Uuid::parse_str(folder_id_str).ok()
        } else {
            None
        };

        // Create folder with default name
        self.app_state.update(cx, |state, cx| {
            state.create_folder("New Folder", parent_id);
            cx.emit(AppStateChanged);
        });

        self.refresh_tree(cx);
    }

    fn create_connection_in_folder(&mut self, _item_id: &str, cx: &mut Context<Self>) {
        // TODO: Open connection manager window with folder context
        // For now, show a toast indicating this feature is pending
        // The connection manager is opened via the main workspace, not directly from sidebar
        self.pending_toast = Some(PendingToast {
            message: "Use the + button in the sidebar header to create a new connection".into(),
            is_error: false,
        });
        cx.notify();
    }

    fn start_rename(&mut self, item_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        // Handle folder rename
        if let Some(folder_id_str) = item_id.strip_prefix("conn_folder_")
            && let Ok(folder_id) = Uuid::parse_str(folder_id_str)
        {
            let current_name = self
                .app_state
                .read(cx)
                .connection_tree
                .find_by_id(folder_id)
                .map(|f| f.name.clone())
                .unwrap_or_default();

            self.editing_id = Some(folder_id);
            self.editing_is_folder = true;
            self.rename_input.update(cx, |input, cx| {
                input.set_value(&current_name, window, cx);
                input.focus(window, cx);
            });
            cx.notify();
            return;
        }

        // Handle profile rename
        if let Some(profile_id_str) = item_id.strip_prefix("profile_")
            && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
        {
            let current_name = self
                .app_state
                .read(cx)
                .profiles
                .iter()
                .find(|p| p.id == profile_id)
                .map(|p| p.name.clone())
                .unwrap_or_default();

            self.editing_id = Some(profile_id);
            self.editing_is_folder = false;
            self.rename_input.update(cx, |input, cx| {
                input.set_value(&current_name, window, cx);
                input.focus(window, cx);
            });
            cx.notify();
        }
    }

    fn delete_folder_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if let Some(folder_id_str) = item_id.strip_prefix("conn_folder_")
            && let Ok(folder_id) = Uuid::parse_str(folder_id_str)
        {
            self.app_state.update(cx, |state, cx| {
                state.delete_folder(folder_id);
                cx.emit(AppStateChanged);
            });

            self.refresh_tree(cx);
        }
    }

    fn move_item_to_folder(
        &mut self,
        item_id: &str,
        target_folder_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) {
        let node_id = if let Some(profile_id_str) = item_id.strip_prefix("profile_")
            && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
        {
            self.app_state
                .read(cx)
                .connection_tree
                .find_by_profile(profile_id)
                .map(|n| n.id)
        } else if let Some(folder_id_str) = item_id.strip_prefix("conn_folder_")
            && let Ok(folder_id) = Uuid::parse_str(folder_id_str)
        {
            Some(folder_id)
        } else {
            None
        };

        if let Some(node_id) = node_id {
            self.app_state.update(cx, |state, cx| {
                if state.move_tree_node(node_id, target_folder_id) {
                    cx.emit(AppStateChanged);
                }
            });
            self.refresh_tree(cx);
        }
    }

    /// Commits the rename operation (folder or profile).
    pub fn commit_rename(&mut self, cx: &mut Context<Self>) {
        let Some(id) = self.editing_id.take() else {
            return;
        };

        let new_name = self.rename_input.read(cx).value().to_string();

        if new_name.trim().is_empty() {
            self.refresh_tree(cx);
            return;
        }

        let is_folder = self.editing_is_folder;

        self.app_state.update(cx, |state, cx| {
            if is_folder {
                if state.rename_folder(id, &new_name) {
                    cx.emit(AppStateChanged);
                }
            } else if let Some(profile) = state.profiles.iter_mut().find(|p| p.id == id) {
                profile.name = new_name;
                state.save_profiles();
                cx.emit(AppStateChanged);
            }
        });

        self.refresh_tree(cx);
        cx.emit(SidebarEvent::RequestFocus);
    }

    /// Cancels the rename operation.
    pub fn cancel_rename(&mut self, cx: &mut Context<Self>) {
        self.editing_id = None;
        cx.emit(SidebarEvent::RequestFocus);
        cx.notify();
    }

    fn handle_drop(
        &mut self,
        drag_state: &SidebarDragState,
        target_parent_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) {
        // Collect all node IDs to move (primary + additional from multi-selection)
        let all_node_ids = drag_state.all_node_ids();

        let tree_node_ids: Vec<Uuid> = {
            let state = self.app_state.read(cx);
            all_node_ids
                .iter()
                .filter_map(|&node_id| {
                    if state.connection_tree.find_by_id(node_id).is_some() {
                        Some(node_id)
                    } else {
                        state.connection_tree.find_by_profile(node_id).map(|n| n.id)
                    }
                })
                .collect()
        };

        let mut moved = false;
        for tree_node_id in tree_node_ids {
            let would_cycle = self
                .app_state
                .read(cx)
                .connection_tree
                .would_create_cycle(tree_node_id, target_parent_id);

            if would_cycle {
                continue;
            }

            self.app_state.update(cx, |state, _cx| {
                if state.move_tree_node(tree_node_id, target_parent_id) {
                    moved = true;
                }
            });
        }

        if moved {
            self.app_state.update(cx, |_, cx| {
                cx.emit(AppStateChanged);
            });
            self.clear_selection(cx);
            self.refresh_tree(cx);
        }
    }

    fn handle_drop_with_position(&mut self, drag_state: &SidebarDragState, cx: &mut Context<Self>) {
        let Some(drop_target) = self.drop_target.take() else {
            return;
        };

        // Collect all node IDs to move (primary + additional from multi-selection)
        let all_node_ids = drag_state.all_node_ids();

        let tree_node_ids: Vec<Uuid> = {
            let state = self.app_state.read(cx);
            all_node_ids
                .iter()
                .filter_map(|&node_id| {
                    if state.connection_tree.find_by_id(node_id).is_some() {
                        Some(node_id)
                    } else {
                        state.connection_tree.find_by_profile(node_id).map(|n| n.id)
                    }
                })
                .collect()
        };

        if tree_node_ids.is_empty() {
            return;
        }

        let (target_parent_id, mut after_id) =
            self.resolve_drop_target(&drop_target.item_id, drop_target.position, cx);

        // Move each node, updating after_id to chain them
        let mut moved = false;
        for tree_node_id in tree_node_ids {
            let would_cycle = self
                .app_state
                .read(cx)
                .connection_tree
                .would_create_cycle(tree_node_id, target_parent_id);

            if would_cycle {
                continue;
            }

            self.app_state.update(cx, |state, _cx| {
                if state.move_tree_node_to_position(tree_node_id, target_parent_id, after_id) {
                    moved = true;
                    // Next node should be placed after this one
                    after_id = Some(tree_node_id);
                }
            });
        }

        if moved {
            self.app_state.update(cx, |_, cx| {
                cx.emit(AppStateChanged);
            });
            self.clear_selection(cx);
            self.refresh_tree(cx);
        }
    }

    /// Resolves a drop target to (parent_id, after_id) for positioning.
    fn resolve_drop_target(
        &self,
        item_id: &str,
        position: DropPosition,
        cx: &App,
    ) -> (Option<Uuid>, Option<Uuid>) {
        let state = self.app_state.read(cx);

        // Parse the target item
        let (target_node_id, is_folder) =
            if let Some(folder_id_str) = item_id.strip_prefix("conn_folder_") {
                (Uuid::parse_str(folder_id_str).ok(), true)
            } else if let Some(profile_id_str) = item_id.strip_prefix("profile_") {
                let profile_id = Uuid::parse_str(profile_id_str).ok();
                let node_id = profile_id
                    .and_then(|pid| state.connection_tree.find_by_profile(pid).map(|n| n.id));
                (node_id, false)
            } else {
                (None, false)
            };

        let Some(target_node_id) = target_node_id else {
            return (None, None);
        };

        let target_node = state.connection_tree.find_by_id(target_node_id);
        let target_parent_id = target_node.and_then(|n| n.parent_id);

        match position {
            DropPosition::Into if is_folder => {
                // Drop into folder: parent is the folder, insert at end
                (Some(target_node_id), None)
            }
            DropPosition::Before => {
                // Drop before: same parent, find the sibling before target
                let siblings = if let Some(pid) = target_parent_id {
                    state.connection_tree.children_of(pid)
                } else {
                    state.connection_tree.root_nodes()
                };
                let pos = siblings.iter().position(|n| n.id == target_node_id);
                let after_id = pos.and_then(|p| {
                    if p > 0 {
                        Some(siblings[p - 1].id)
                    } else {
                        None
                    }
                });
                (target_parent_id, after_id)
            }
            DropPosition::After | DropPosition::Into => {
                // Drop after (or Into non-folder): same parent, after target
                (target_parent_id, Some(target_node_id))
            }
        }
    }

    fn set_drop_target(&mut self, item_id: String, position: DropPosition, cx: &mut Context<Self>) {
        let new_target = DropTarget { item_id, position };
        if self.drop_target.as_ref() != Some(&new_target) {
            self.drop_target = Some(new_target);
            cx.notify();
        }
    }

    fn clear_drop_target(&mut self, cx: &mut Context<Self>) {
        if self.drop_target.is_some() {
            self.drop_target = None;
            cx.notify();
        }
    }

    /// Starts tracking hover over a folder during drag for auto-expand.
    fn start_drag_hover_folder(&mut self, folder_id: Uuid, cx: &mut Context<Self>) {
        if self.drag_hover_folder != Some(folder_id) {
            self.drag_hover_folder = Some(folder_id);
            self.drag_hover_start = Some(std::time::Instant::now());

            // Schedule a check after the delay
            let delay = std::time::Duration::from_millis(600);
            cx.spawn(async move |this, cx| {
                cx.background_executor().timer(delay).await;
                let _ = this.update(cx, |this, cx| {
                    this.check_auto_expand_folder(cx);
                });
            })
            .detach();
        }
    }

    /// Clears the drag hover tracking.
    fn clear_drag_hover_folder(&mut self, cx: &mut Context<Self>) {
        if self.drag_hover_folder.is_some() {
            self.drag_hover_folder = None;
            self.drag_hover_start = None;
            cx.notify();
        }
    }

    /// Checks if a folder should be auto-expanded after hover delay.
    fn check_auto_expand_folder(&mut self, cx: &mut Context<Self>) {
        let Some(folder_id) = self.drag_hover_folder else {
            return;
        };

        let Some(hover_start) = self.drag_hover_start else {
            return;
        };

        // Check if we've been hovering long enough (600ms)
        if hover_start.elapsed() >= std::time::Duration::from_millis(600) {
            // Check if the folder is collapsed
            let is_collapsed = self
                .app_state
                .read(cx)
                .connection_tree
                .find_by_id(folder_id)
                .map(|n| n.collapsed)
                .unwrap_or(false);

            if is_collapsed {
                self.app_state.update(cx, |state, _cx| {
                    state.set_folder_collapsed(folder_id, false);
                });
                self.refresh_tree(cx);
            }
        }
    }

    /// Checks if we should auto-scroll based on the hovered item index.
    fn check_auto_scroll(&mut self, item_index: usize, cx: &mut Context<Self>) {
        let total = self.visible_entry_count;
        if total == 0 {
            return;
        }

        // Scroll up if hovering near the top (first 2 items)
        // Scroll down if hovering near the bottom (last 2 items)
        let new_direction = if item_index <= 1 {
            -1 // Scroll up
        } else if item_index >= total.saturating_sub(2) {
            1 // Scroll down
        } else {
            0 // No scroll
        };

        if new_direction != self.auto_scroll_direction {
            self.auto_scroll_direction = new_direction;

            if new_direction != 0 {
                // Start auto-scroll timer
                cx.spawn(async move |this, cx| {
                    Self::auto_scroll_loop(this, cx).await;
                })
                .detach();
            }
        }
    }

    /// Continuously scrolls while auto_scroll_direction is non-zero.
    async fn auto_scroll_loop(this: WeakEntity<Self>, cx: &mut AsyncApp) {
        let interval = std::time::Duration::from_millis(50);

        loop {
            cx.background_executor().timer(interval).await;

            let should_continue = this
                .update(cx, |this, cx| {
                    if this.auto_scroll_direction == 0 {
                        return false;
                    }

                    this.do_auto_scroll(cx);
                    true
                })
                .unwrap_or(false);

            if !should_continue {
                break;
            }
        }
    }

    /// Performs one step of auto-scroll.
    fn do_auto_scroll(&mut self, cx: &mut Context<Self>) {
        let direction = self.auto_scroll_direction;
        if direction == 0 {
            return;
        }

        self.tree_state.update(cx, |state, cx| {
            let current = state.selected_index().unwrap_or(0);
            let total = self.visible_entry_count;

            let target = if direction < 0 {
                // Scroll up
                current.saturating_sub(1)
            } else {
                // Scroll down
                (current + 1).min(total.saturating_sub(1))
            };

            state.scroll_to_item(target, gpui::ScrollStrategy::Top);
            cx.notify();
        });
    }

    /// Stops auto-scrolling.
    fn stop_auto_scroll(&mut self, _cx: &mut Context<Self>) {
        self.auto_scroll_direction = 0;
    }

    fn toggle_selection(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if !Self::is_selectable_item(item_id) {
            return;
        }

        if self.multi_selection.contains(item_id) {
            self.multi_selection.remove(item_id);
        } else {
            self.multi_selection.insert(item_id.to_string());
        }
        cx.notify();
    }

    fn add_to_selection(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if !Self::is_selectable_item(item_id) {
            return;
        }

        if self.multi_selection.insert(item_id.to_string()) {
            cx.notify();
        }
    }

    pub fn clear_selection(&mut self, cx: &mut Context<Self>) {
        if !self.multi_selection.is_empty() {
            self.multi_selection.clear();
            cx.notify();
        }
    }

    pub fn start_rename_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() else {
            return;
        };

        let item_id = entry.item().id.to_string();
        let kind = TreeNodeKind::from_id(&item_id);

        if matches!(kind, TreeNodeKind::ConnectionFolder | TreeNodeKind::Profile) {
            self.start_rename(&item_id, window, cx);
        }
    }

    pub fn request_delete_selected(&mut self, cx: &mut Context<Self>) {
        if self.pending_delete_item.is_some() {
            self.confirm_pending_delete(cx);
            return;
        }

        let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() else {
            return;
        };

        let item_id = entry.item().id.to_string();
        let kind = TreeNodeKind::from_id(&item_id);

        if matches!(kind, TreeNodeKind::ConnectionFolder | TreeNodeKind::Profile) {
            self.pending_delete_item = Some(item_id);
            cx.notify();
        }
    }

    fn confirm_pending_delete(&mut self, cx: &mut Context<Self>) {
        let Some(item_id) = self.pending_delete_item.take() else {
            return;
        };

        self.execute_delete(&item_id, cx);
    }

    pub fn cancel_pending_delete(&mut self, cx: &mut Context<Self>) {
        if self.pending_delete_item.is_some() {
            self.pending_delete_item = None;
            cx.notify();
        }
    }

    pub fn has_pending_delete(&self) -> bool {
        self.pending_delete_item.is_some()
    }

    pub fn show_delete_confirm_modal(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let kind = TreeNodeKind::from_id(item_id);
        let state = self.app_state.read(cx);

        let (item_name, is_folder) = match kind {
            TreeNodeKind::ConnectionFolder => {
                if let Some(folder_id_str) = item_id.strip_prefix("conn_folder_")
                    && let Ok(folder_id) = Uuid::parse_str(folder_id_str)
                    && let Some(node) = state.connection_tree.find_by_id(folder_id)
                {
                    (node.name.clone(), true)
                } else {
                    return;
                }
            }
            TreeNodeKind::Profile => {
                if let Some(profile_id_str) = item_id.strip_prefix("profile_")
                    && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
                    && let Some(profile) = state.profiles.iter().find(|p| p.id == profile_id)
                {
                    (profile.name.clone(), false)
                } else {
                    return;
                }
            }
            _ => return,
        };

        self.delete_confirm_modal = Some(DeleteConfirmState {
            item_id: item_id.to_string(),
            item_name,
            is_folder,
        });
        cx.notify();
    }

    pub fn confirm_modal_delete(&mut self, cx: &mut Context<Self>) {
        let Some(modal) = self.delete_confirm_modal.take() else {
            return;
        };

        self.execute_delete(&modal.item_id, cx);
    }

    pub fn cancel_modal_delete(&mut self, cx: &mut Context<Self>) {
        if self.delete_confirm_modal.is_some() {
            self.delete_confirm_modal = None;
            cx.notify();
        }
    }

    pub fn has_delete_modal(&self) -> bool {
        self.delete_confirm_modal.is_some()
    }

    pub fn delete_modal_info(&self) -> Option<(&str, bool)> {
        self.delete_confirm_modal
            .as_ref()
            .map(|m| (m.item_name.as_str(), m.is_folder))
    }

    pub fn toggle_add_menu(&mut self, cx: &mut Context<Self>) {
        self.add_menu_open = !self.add_menu_open;
        cx.notify();
    }

    pub fn close_add_menu(&mut self, cx: &mut Context<Self>) {
        if self.add_menu_open {
            self.add_menu_open = false;
            cx.notify();
        }
    }

    #[allow(dead_code)]
    pub fn is_add_menu_open(&self) -> bool {
        self.add_menu_open
    }

    fn execute_delete(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let kind = TreeNodeKind::from_id(item_id);

        match kind {
            TreeNodeKind::ConnectionFolder => {
                self.delete_folder_from_context(item_id, cx);
            }
            TreeNodeKind::Profile => {
                if let Some(profile_id_str) = item_id.strip_prefix("profile_")
                    && let Ok(profile_id) = Uuid::parse_str(profile_id_str)
                {
                    self.delete_profile(profile_id, cx);
                }
            }
            _ => {}
        }

        self.refresh_tree(cx);
    }

    #[allow(dead_code)]
    fn is_multi_selected(&self, item_id: &str) -> bool {
        self.multi_selection.contains(item_id)
    }

    fn is_selectable_item(item_id: &str) -> bool {
        item_id.starts_with("profile_") || item_id.starts_with("conn_folder_")
    }

    #[allow(dead_code)]
    fn extend_selection_to_index(&mut self, target_index: usize, cx: &mut Context<Self>) {
        if target_index >= self.visible_entry_count {
            return;
        }

        // Update tree selection
        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(target_index), cx);
            state.scroll_to_item(target_index, gpui::ScrollStrategy::Center);
        });

        // Add the selected item
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }
    }

    pub fn move_selected_items(&mut self, direction: i32, cx: &mut Context<Self>) {
        if self.multi_selection.is_empty() {
            return;
        }

        // Collect node IDs from selection
        let state = self.app_state.read(cx);
        let mut nodes_to_move: Vec<(Uuid, i32)> = Vec::new();

        for item_id in &self.multi_selection {
            if let Some(node_id) = self.item_id_to_node_id(item_id, &state.connection_tree)
                && let Some(node) = state.connection_tree.find_by_id(node_id)
            {
                nodes_to_move.push((node_id, node.sort_index));
            }
        }

        if nodes_to_move.is_empty() {
            return;
        }

        // Sort by current sort_index
        nodes_to_move.sort_by_key(|(_, idx)| *idx);

        // If moving up, process from top to bottom
        // If moving down, process from bottom to top
        if direction > 0 {
            nodes_to_move.reverse();
        }

        let _ = state;

        let mut moved = false;
        for (node_id, _) in nodes_to_move {
            if self.move_single_node(node_id, direction, cx) {
                moved = true;
            }
        }

        if moved {
            self.app_state.update(cx, |_, cx| {
                cx.emit(AppStateChanged);
            });
            self.refresh_tree(cx);
        }
    }

    fn move_single_node(&mut self, node_id: Uuid, direction: i32, cx: &mut Context<Self>) -> bool {
        let state = self.app_state.read(cx);
        let tree = &state.connection_tree;

        let node = match tree.find_by_id(node_id) {
            Some(n) => n.clone(),
            None => return false,
        };

        // Get siblings
        let siblings: Vec<_> = if let Some(parent_id) = node.parent_id {
            tree.children_of(parent_id)
        } else {
            tree.root_nodes()
        };

        // Find current position
        let current_pos = match siblings.iter().position(|n| n.id == node_id) {
            Some(p) => p,
            None => return false,
        };

        // Calculate new position
        let new_pos = if direction < 0 {
            if current_pos == 0 {
                return false;
            }
            current_pos - 1
        } else {
            if current_pos >= siblings.len() - 1 {
                return false;
            }
            current_pos + 1
        };

        // Get the sibling we're swapping with
        let swap_with = siblings[new_pos].id;
        let swap_sort_index = siblings[new_pos].sort_index;
        let node_sort_index = node.sort_index;

        let _ = state;

        // Swap sort indices
        self.app_state.update(cx, |state, _cx| {
            if let Some(n) = state.connection_tree.find_by_id_mut(node_id) {
                n.sort_index = swap_sort_index;
            }
            if let Some(n) = state.connection_tree.find_by_id_mut(swap_with) {
                n.sort_index = node_sort_index;
            }
            state.save_connection_tree();
        });

        true
    }

    fn item_id_to_node_id(
        &self,
        item_id: &str,
        tree: &dbflux_core::ConnectionTree,
    ) -> Option<Uuid> {
        if let Some(folder_id_str) = item_id.strip_prefix("conn_folder_") {
            Uuid::parse_str(folder_id_str).ok()
        } else if let Some(profile_id_str) = item_id.strip_prefix("profile_") {
            let profile_id = Uuid::parse_str(profile_id_str).ok()?;
            tree.find_by_profile(profile_id).map(|n| n.id)
        } else {
            None
        }
    }

    /// Returns true if currently renaming an item.
    pub fn is_renaming(&self) -> bool {
        self.editing_id.is_some()
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

    /// Remove expansion overrides for folders whose data hasn't been loaded yet.
    fn cleanup_stale_overrides(&mut self, cx: &Context<Self>) {
        let state = self.app_state.read(cx);

        self.expansion_overrides.retain(|item_id, _expanded| {
            if item_id.starts_with("types_")
                && let Some((profile_id, database, schema)) =
                    Self::parse_schema_folder_id(item_id, "types_")
            {
                return !state.needs_schema_types(profile_id, &database, Some(&schema));
            }

            if item_id.starts_with("schema_indexes_")
                && let Some((profile_id, database, schema)) =
                    Self::parse_schema_folder_id(item_id, "schema_indexes_")
            {
                return !state.needs_schema_indexes(profile_id, &database, Some(&schema));
            }

            if item_id.starts_with("schema_fks_")
                && let Some((profile_id, database, schema)) =
                    Self::parse_schema_folder_id(item_id, "schema_fks_")
            {
                return !state.needs_schema_foreign_keys(profile_id, &database, Some(&schema));
            }

            true
        });
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
        let root_nodes = state.connection_tree.root_nodes();
        Self::build_tree_nodes_recursive(&root_nodes, state)
    }

    fn build_tree_nodes_recursive(
        nodes: &[&ConnectionTreeNode],
        state: &AppState,
    ) -> Vec<TreeItem> {
        let mut items = Vec::new();

        for node in nodes {
            match node.kind {
                ConnectionTreeNodeKind::Folder => {
                    let children_nodes = state.connection_tree.children_of(node.id);
                    let children_refs: Vec<&ConnectionTreeNode> =
                        children_nodes.into_iter().collect();
                    let children = Self::build_tree_nodes_recursive(&children_refs, state);

                    let folder_item =
                        TreeItem::new(format!("conn_folder_{}", node.id), node.name.clone())
                            .expanded(!node.collapsed)
                            .children(children);

                    items.push(folder_item);
                }

                ConnectionTreeNodeKind::ConnectionRef => {
                    if let Some(profile_id) = node.profile_id
                        && let Some(profile) = state.profiles.iter().find(|p| p.id == profile_id)
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
                                &db.name,
                                db_schema,
                                &connected.table_details,
                                &connected.schema_types,
                                &connected.schema_indexes,
                                &connected.schema_foreign_keys,
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
                            &db.name,
                            schema,
                            &connected.table_details,
                            &connected.schema_types,
                            &connected.schema_indexes,
                            &connected.schema_foreign_keys,
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
                // No databases defined - use active_database or first schema as fallback
                let database_name = connected
                    .active_database
                    .as_deref()
                    .or_else(|| schema.schemas.first().map(|s| s.name.as_str()))
                    .unwrap_or("default");

                profile_children = Self::build_schema_children(
                    profile_id,
                    database_name,
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
        database_name: &str,
        snapshot: &dbflux_core::SchemaSnapshot,
        table_details: &HashMap<(String, String), TableInfo>,
        schema_types: &HashMap<String, Vec<CustomTypeInfo>>,
        schema_indexes: &HashMap<String, Vec<SchemaIndexInfo>>,
        schema_foreign_keys: &HashMap<String, Vec<SchemaForeignKeyInfo>>,
    ) -> Vec<TreeItem> {
        let mut children = Vec::new();

        for db_schema in &snapshot.schemas {
            let schema_content = Self::build_db_schema_content(
                profile_id,
                database_name,
                db_schema,
                table_details,
                schema_types,
                schema_indexes,
                schema_foreign_keys,
            );

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
        database_name: &str,
        db_schema: &dbflux_core::DbSchemaInfo,
        table_details: &HashMap<(String, String), TableInfo>,
        schema_types: &HashMap<String, Vec<CustomTypeInfo>>,
        schema_indexes: &HashMap<String, Vec<SchemaIndexInfo>>,
        schema_foreign_keys: &HashMap<String, Vec<SchemaForeignKeyInfo>>,
    ) -> Vec<TreeItem> {
        let mut content = Vec::new();
        let schema_name = &db_schema.name;

        if !db_schema.tables.is_empty() {
            let table_children: Vec<TreeItem> = db_schema
                .tables
                .iter()
                .map(|table| Self::build_table_item(profile_id, schema_name, table, table_details))
                .collect();

            content.push(
                TreeItem::new(
                    format!("tables_{}_{}", profile_id, schema_name),
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
                        format!("view_{}__{}__{}", profile_id, schema_name, view.name),
                        view.name.clone(),
                    )
                })
                .collect();

            content.push(
                TreeItem::new(
                    format!("views_{}_{}", profile_id, schema_name),
                    format!("Views ({})", db_schema.views.len()),
                )
                .expanded(true)
                .children(view_children),
            );
        }

        // Custom types (enums, domains, composites) - check cache first, then db_schema
        let types_cache_key = format!("{}__{}", database_name, schema_name);
        let cached_types = schema_types.get(&types_cache_key);

        let custom_types: Option<&Vec<CustomTypeInfo>> =
            cached_types.or(db_schema.custom_types.as_ref());

        // Item ID format: types_{profile_id}_{database}_{schema}
        let types_item_id = format!("types_{}_{}_{}", profile_id, database_name, schema_name);

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
                format!(
                    "types_loading_{}_{}_{}",
                    profile_id, database_name, schema_name
                ),
                "Loading...".to_string(),
            );

            content.push(
                TreeItem::new(types_item_id, "Data Types".to_string())
                    .expanded(false)
                    .children(vec![placeholder]),
            );
        }

        // Schema-level Indexes folder
        let indexes_cache_key = format!("{}__{}", database_name, schema_name);
        let cached_indexes = schema_indexes.get(&indexes_cache_key);
        let indexes_item_id = format!(
            "schema_indexes_{}_{}_{}",
            profile_id, database_name, schema_name
        );

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
                            format!("sidx_{}__{}__{}", profile_id, schema_name, idx.name),
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
                format!(
                    "schema_indexes_loading_{}_{}_{}",
                    profile_id, database_name, schema_name
                ),
                "Loading...".to_string(),
            );

            content.push(
                TreeItem::new(indexes_item_id, "Indexes".to_string())
                    .expanded(false)
                    .children(vec![placeholder]),
            );
        }

        // Schema-level Foreign Keys folder
        let fks_cache_key = format!("{}__{}", database_name, schema_name);
        let cached_fks = schema_foreign_keys.get(&fks_cache_key);
        let fks_item_id = format!(
            "schema_fks_{}_{}_{}",
            profile_id, database_name, schema_name
        );

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
                            format!("sfk_{}__{}__{}", profile_id, schema_name, fk.name),
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
                format!(
                    "schema_fks_loading_{}_{}_{}",
                    profile_id, database_name, schema_name
                ),
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
                        format!(
                            "enumval_{}__{}__{}_{}",
                            profile_id, schema_name, custom_type.name, v
                        ),
                        v.clone(),
                    )
                })
                .collect();
        }

        // For domains, show the base type as a child
        if let Some(ref base_type) = custom_type.base_type {
            children.push(TreeItem::new(
                format!(
                    "basetype_{}__{}__{}",
                    profile_id, schema_name, custom_type.name
                ),
                format!("Base: {}", base_type),
            ));
        }

        TreeItem::new(
            format!(
                "customtype_{}__{}__{}",
                profile_id, schema_name, custom_type.name
            ),
            label,
        )
        .expanded(false)
        .children(children)
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

        // foreign_keys: None = not loaded yet, Some([]) = loaded but empty
        if let Some(ref fks) = effective_table.foreign_keys
            && !fks.is_empty()
        {
            let fk_children: Vec<TreeItem> = fks
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
                        format!("fk_{}__{}__{}", profile_id, table.name, fk.name),
                        label,
                    )
                })
                .collect();

            table_sections.push(
                TreeItem::new(
                    format!("fks_{}__{}__{}", profile_id, schema_name, table.name),
                    format!("Foreign Keys ({})", fks.len()),
                )
                .expanded(false)
                .children(fk_children),
            );
        }

        // constraints: None = not loaded yet, Some([]) = loaded but empty
        if let Some(ref constraints) = effective_table.constraints
            && !constraints.is_empty()
        {
            let constraint_children: Vec<TreeItem> = constraints
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
                        format!("constraint_{}__{}__{}", profile_id, table.name, c.name),
                        label,
                    )
                })
                .collect();

            table_sections.push(
                TreeItem::new(
                    format!(
                        "constraints_{}__{}__{}",
                        profile_id, schema_name, table.name
                    ),
                    format!("Constraints ({})", constraints.len()),
                )
                .expanded(false)
                .children(constraint_children),
            );
        }

        // Add placeholder when columns not loaded yet (shows chevron indicator)
        if columns_not_loaded && table_sections.is_empty() {
            table_sections.push(TreeItem::new(
                format!(
                    "placeholder_{}__{}__{}",
                    profile_id, schema_name, table.name
                ),
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
                app_id: Some("dbflux".into()),
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
                let icon = item.action.icon();
                let sidebar_for_click = sidebar.clone();
                let item_id = SharedString::from(format!("{}-item-{}", panel_id, idx));

                let icon_color = if is_selected {
                    theme.accent_foreground
                } else {
                    theme.muted_foreground
                };

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
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .when_some(icon, |d, icon| {
                                d.child(svg().path(icon.path()).size_4().text_color(icon_color))
                            })
                            .child(item.label.clone()),
                    )
                    .when(is_submenu, |d| {
                        d.child(
                            svg()
                                .path(AppIcon::ChevronRight.path())
                                .size_4()
                                .text_color(theme.muted_foreground),
                        )
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
                                app_id: Some("dbflux".into()),
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
                        svg()
                            .path(AppIcon::Settings.path())
                            .size_4()
                            .text_color(theme.muted_foreground),
                    )
                    .child("Settings"),
            )
    }
}

impl Render for Sidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(toast) = self.pending_toast.take() {
            use crate::ui::toast::ToastExt;
            if toast.is_error {
                cx.toast_error(toast.message, window);
            } else {
                cx.toast_success(toast.message, window);
            }
        }

        if let Some(item_id) = self.pending_rename_item.take() {
            self.start_rename(&item_id, window, cx);
        }

        let theme = cx.theme();
        let state = self.app_state.read(cx);
        let active_id = state.active_connection_id;
        let connections = state.connections.keys().copied().collect::<Vec<_>>();

        // Pre-compute profile_id -> Icon map for use in the tree closure
        // (closure requires 'static, so we can't borrow state inside it)
        let profile_icons: HashMap<Uuid, dbflux_core::Icon> = state
            .profiles
            .iter()
            .filter_map(|p| {
                state
                    .drivers
                    .get(&p.kind())
                    .map(|driver| (p.id, driver.metadata().icon))
            })
            .collect();

        let active_databases = self.active_databases.clone();
        let sidebar_entity = cx.entity().clone();
        let multi_selection = self.multi_selection.clone();
        let pending_delete = self.pending_delete_item.clone();

        let color_teal: Hsla = gpui::rgb(0x4EC9B0).into();
        let color_yellow: Hsla = gpui::rgb(0xDCDCAA).into();
        let color_blue: Hsla = gpui::rgb(0x9CDCFE).into();
        let color_purple: Hsla = gpui::rgb(0xC586C0).into();
        let color_gray: Hsla = gpui::rgb(0x808080).into();
        let color_orange: Hsla = gpui::rgb(0xCE9178).into();
        let color_schema: Hsla = gpui::rgb(0x569CD6).into();
        let color_green: Hsla = gpui::green();

        div()
            .relative()
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
                    .child({
                        let sidebar_for_toggle = sidebar_entity.clone();
                        let hover_bg = theme.secondary;
                        div()
                            .id("add-button")
                            .w(Heights::ICON_LG)
                            .h(Heights::ICON_LG)
                            .flex()
                            .items_center()
                            .justify_center()
                            .rounded(Radii::SM)
                            .text_size(FontSizes::LG)
                            .text_color(theme.muted_foreground)
                            .cursor_pointer()
                            .hover(move |d| d.bg(hover_bg).text_color(theme.foreground))
                            .on_click(move |_, _, cx| {
                                sidebar_for_toggle.update(cx, |this, cx| {
                                    this.toggle_add_menu(cx);
                                });
                            })
                            .child("+")
                    }),
            )
            .when(self.pending_delete_item.is_some(), |el| {
                el.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(gpui::rgb(0x5C1F1F))
                        .flex()
                        .items_center()
                        .justify_center()
                        .text_size(FontSizes::XS)
                        .text_color(theme.foreground)
                        .child("Press x to confirm delete, ESC to cancel"),
                )
            })
            .when(self.editing_id.is_some(), |el| {
                let rename_input = self.rename_input.clone();
                let sidebar_confirm = sidebar_entity.clone();
                let sidebar_cancel = sidebar_entity.clone();

                el.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(theme.sidebar)
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(
                            div().flex_1().child(
                                Input::new(&rename_input)
                                    .xsmall()
                                    .appearance(false)
                                    .cleanable(false),
                            ),
                        )
                        .child(
                            div()
                                .id("rename-confirm")
                                .px(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(color_green)
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(move |_, _, cx| {
                                    sidebar_confirm.update(cx, |this, cx| {
                                        this.commit_rename(cx);
                                    });
                                })
                                .child("✓"),
                        )
                        .child(
                            div()
                                .id("rename-cancel")
                                .px(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(move |_, _, cx| {
                                    sidebar_cancel.update(cx, |this, cx| {
                                        this.cancel_rename(cx);
                                    });
                                })
                                .child("✕"),
                        ),
                )
            })
            .child({
                let sidebar_for_root_drop = sidebar_entity.clone();
                let sidebar_for_clear_drop = sidebar_entity.clone();
                let current_drop_target = self.drop_target.clone();
                let drop_indicator_color = theme.accent;

                div()
                    .flex_1()
                    .overflow_hidden()
                    .on_drop(move |state: &SidebarDragState, _, cx| {
                        sidebar_for_root_drop.update(cx, |this, cx| {
                            this.stop_auto_scroll(cx);
                            this.clear_drop_target(cx);
                            this.clear_drag_hover_folder(cx);
                            this.handle_drop(state, None, cx);
                        });
                    })
                    .on_drag_move::<SidebarDragState>(move |_, _, cx| {
                        sidebar_for_clear_drop.update(cx, |this, cx| {
                            this.stop_auto_scroll(cx);
                            this.clear_drop_target(cx);
                            this.clear_drag_hover_folder(cx);
                        });
                    })
                    .child(tree(
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
                            let indent_per_level = 12.0_f32;
                            let is_folder = entry.is_folder();
                            let is_expanded = entry.is_expanded();

                            let needs_chevron = is_folder
                                && matches!(
                                    node_kind,
                                    TreeNodeKind::ConnectionFolder
                                        | TreeNodeKind::Table
                                        | TreeNodeKind::View
                                        | TreeNodeKind::Schema
                                        | TreeNodeKind::TablesFolder
                                        | TreeNodeKind::ViewsFolder
                                        | TreeNodeKind::TypesFolder
                                        | TreeNodeKind::ColumnsFolder
                                        | TreeNodeKind::IndexesFolder
                                        | TreeNodeKind::ForeignKeysFolder
                                        | TreeNodeKind::ConstraintsFolder
                                        | TreeNodeKind::SchemaIndexesFolder
                                        | TreeNodeKind::SchemaForeignKeysFolder
                                        | TreeNodeKind::CustomType
                                        | TreeNodeKind::Database
                                        | TreeNodeKind::Profile
                                );
                            let chevron_icon: Option<AppIcon> = if needs_chevron {
                                Some(if is_expanded {
                                    AppIcon::ChevronDown
                                } else {
                                    AppIcon::ChevronRight
                                })
                            } else {
                                None
                            };

                            let (node_icon, unicode_icon, icon_color): (
                                Option<AppIcon>,
                                &str,
                                Hsla,
                            ) = match node_kind {
                                TreeNodeKind::ConnectionFolder => {
                                    (Some(AppIcon::Folder), "", theme.muted_foreground)
                                }
                                TreeNodeKind::Profile => {
                                    let icon = item_id
                                        .strip_prefix("profile_")
                                        .and_then(|id_str| Uuid::parse_str(id_str).ok())
                                        .and_then(|id| profile_icons.get(&id).copied())
                                        .map(AppIcon::from_icon);

                                    let color = if is_connected {
                                        color_green
                                    } else {
                                        theme.muted_foreground
                                    };
                                    let unicode = if icon.is_none() {
                                        if is_connected { "●" } else { "○" }
                                    } else {
                                        ""
                                    };
                                    (icon, unicode, color)
                                }
                                TreeNodeKind::Database => {
                                    (Some(AppIcon::Database), "", color_orange)
                                }
                                TreeNodeKind::Schema => (Some(AppIcon::Layers), "", color_schema),
                                TreeNodeKind::TablesFolder => {
                                    (Some(AppIcon::Table), "", color_teal)
                                }
                                TreeNodeKind::ViewsFolder => (Some(AppIcon::Eye), "", color_yellow),
                                TreeNodeKind::TypesFolder => {
                                    (Some(AppIcon::Braces), "", color_purple)
                                }
                                TreeNodeKind::Table => (Some(AppIcon::Table), "", color_teal),
                                TreeNodeKind::View => (Some(AppIcon::Eye), "", color_yellow),
                                TreeNodeKind::CustomType => {
                                    (Some(AppIcon::Braces), "", color_purple)
                                }
                                TreeNodeKind::ColumnsFolder => {
                                    (Some(AppIcon::Columns), "", color_blue)
                                }
                                TreeNodeKind::IndexesFolder | TreeNodeKind::SchemaIndexesFolder => {
                                    (Some(AppIcon::Hash), "", color_purple)
                                }
                                TreeNodeKind::ForeignKeysFolder
                                | TreeNodeKind::SchemaForeignKeysFolder => {
                                    (Some(AppIcon::KeyRound), "", color_orange)
                                }
                                TreeNodeKind::ConstraintsFolder => {
                                    (Some(AppIcon::Lock), "", color_yellow)
                                }
                                TreeNodeKind::Column => (Some(AppIcon::Columns), "", color_blue),
                                TreeNodeKind::Index | TreeNodeKind::SchemaIndex => {
                                    (Some(AppIcon::Hash), "", color_purple)
                                }
                                TreeNodeKind::ForeignKey | TreeNodeKind::SchemaForeignKey => {
                                    (Some(AppIcon::KeyRound), "", color_orange)
                                }
                                TreeNodeKind::Constraint => (Some(AppIcon::Lock), "", color_yellow),
                                TreeNodeKind::Unknown => (None, "", theme.muted_foreground),
                            };

                            let label_color: Hsla = match node_kind {
                                TreeNodeKind::ConnectionFolder => theme.foreground,
                                TreeNodeKind::Profile => theme.foreground,
                                TreeNodeKind::Database => color_orange,
                                TreeNodeKind::Schema => color_schema,
                                TreeNodeKind::TablesFolder
                                | TreeNodeKind::ViewsFolder
                                | TreeNodeKind::TypesFolder
                                | TreeNodeKind::ColumnsFolder
                                | TreeNodeKind::IndexesFolder
                                | TreeNodeKind::ForeignKeysFolder
                                | TreeNodeKind::ConstraintsFolder
                                | TreeNodeKind::SchemaIndexesFolder
                                | TreeNodeKind::SchemaForeignKeysFolder => color_gray,
                                TreeNodeKind::Table => color_teal,
                                TreeNodeKind::View => color_yellow,
                                TreeNodeKind::CustomType => color_purple,
                                TreeNodeKind::Column => color_blue,
                                TreeNodeKind::Index | TreeNodeKind::SchemaIndex => color_purple,
                                TreeNodeKind::ForeignKey | TreeNodeKind::SchemaForeignKey => {
                                    color_orange
                                }
                                TreeNodeKind::Constraint => color_yellow,
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

                            let guide_lines: Vec<_> = (0..depth)
                                .map(|_| {
                                    div()
                                        .w(px(indent_per_level))
                                        .h_full()
                                        .flex()
                                        .justify_center()
                                        .child(div().w(px(1.0)).h_full().bg(theme.border))
                                })
                                .collect();

                            let is_multi_selected = multi_selection.contains(item_id.as_ref());
                            let multi_select_bg = theme.list_active;

                            let is_pending_delete = pending_delete
                                .as_ref()
                                .is_some_and(|id| id == item_id.as_ref());
                            let pending_delete_bg: Hsla = gpui::rgb(0x5C1F1F).into();

                            let mut list_item = ListItem::new(ix)
                                .selected(selected)
                                .py(Spacing::XS)
                                .when(is_pending_delete, |el| el.bg(pending_delete_bg))
                                .when(is_multi_selected && !selected && !is_pending_delete, |el| {
                                    el.bg(multi_select_bg)
                                })
                                .child(
                                    div()
                                        .id(SharedString::from(format!("row-{}", item_id)))
                                        .w_full()
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::XS)
                                        .children(guide_lines)
                                        .when(is_table_or_view, |el| {
                                            let sidebar_md = sidebar_for_mousedown.clone();
                                            let id_md = item_id_for_mousedown.clone();
                                            let sidebar_cl = sidebar_for_click.clone();
                                            let id_cl = item_id_for_click.clone();
                                            el.on_mouse_down(MouseButton::Left, move |_, _, cx| {
                                                cx.stop_propagation();
                                                sidebar_md.update(cx, |this, cx| {
                                                    if let Some(idx) =
                                                        this.find_item_index(&id_md, cx)
                                                    {
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
                                                .id(SharedString::from(format!(
                                                    "chevron-{}",
                                                    item_id
                                                )))
                                                .w(px(12.0))
                                                .flex()
                                                .justify_center()
                                                .when_some(chevron_icon, |el, icon| {
                                                    el.cursor_pointer()
                                                        .on_mouse_down(
                                                            MouseButton::Left,
                                                            |_, _, cx| {
                                                                cx.stop_propagation();
                                                            },
                                                        )
                                                        .on_click(move |_, _, cx| {
                                                            cx.stop_propagation();
                                                            sidebar_for_chevron.update(
                                                                cx,
                                                                |this, cx| {
                                                                    this.toggle_item_expansion(
                                                                        &item_id_for_chevron,
                                                                        cx,
                                                                    );
                                                                },
                                                            );
                                                        })
                                                        .child(
                                                            svg()
                                                                .path(icon.path())
                                                                .size_3()
                                                                .text_color(theme.muted_foreground),
                                                        )
                                                }),
                                        )
                                        .child(
                                            div()
                                                .w(Heights::ICON_SM)
                                                .flex()
                                                .justify_center()
                                                .when_some(node_icon, |el, icon| {
                                                    el.child(
                                                        svg()
                                                            .path(icon.path())
                                                            .size_3p5()
                                                            .text_color(icon_color),
                                                    )
                                                })
                                                .when(
                                                    node_icon.is_none() && !unicode_icon.is_empty(),
                                                    |el| {
                                                        el.text_size(FontSizes::SM)
                                                            .text_color(icon_color)
                                                            .child(unicode_icon)
                                                    },
                                                ),
                                        )
                                        .child(
                                            div()
                                                .flex_1()
                                                .overflow_hidden()
                                                .text_ellipsis()
                                                .text_size(FontSizes::SM)
                                                .text_color(label_color)
                                                .when(
                                                    node_kind == TreeNodeKind::Profile && is_active,
                                                    |d| d.font_weight(FontWeight::SEMIBOLD),
                                                )
                                                .when(is_active_database, |d| {
                                                    d.font_weight(FontWeight::SEMIBOLD)
                                                })
                                                .when(
                                                    matches!(
                                                        node_kind,
                                                        TreeNodeKind::TablesFolder
                                                            | TreeNodeKind::ViewsFolder
                                                            | TreeNodeKind::TypesFolder
                                                            | TreeNodeKind::ColumnsFolder
                                                            | TreeNodeKind::IndexesFolder
                                                            | TreeNodeKind::ForeignKeysFolder
                                                            | TreeNodeKind::ConstraintsFolder
                                                    ),
                                                    |d| d.font_weight(FontWeight::MEDIUM),
                                                )
                                                .child(item.label.clone()),
                                        )
                                        .when(
                                            matches!(
                                                node_kind,
                                                TreeNodeKind::Profile
                                                    | TreeNodeKind::ConnectionFolder
                                            ),
                                            |el| {
                                                let drag_node_id = match node_kind {
                                                    TreeNodeKind::Profile => item_id
                                                        .strip_prefix("profile_")
                                                        .and_then(|s| Uuid::parse_str(s).ok()),
                                                    TreeNodeKind::ConnectionFolder => item_id
                                                        .strip_prefix("conn_folder_")
                                                        .and_then(|s| Uuid::parse_str(s).ok()),
                                                    _ => None,
                                                };

                                                if let Some(node_id) = drag_node_id {
                                                    let drag_label = item.label.to_string();
                                                    let is_folder =
                                                        node_kind == TreeNodeKind::ConnectionFolder;

                                                    // Collect additional nodes from multi-selection
                                                    let current_item_id = item_id.to_string();
                                                    let additional_nodes: Vec<Uuid> =
                                                        multi_selection
                                                            .iter()
                                                            .filter(|id| *id != &current_item_id)
                                                            .filter_map(|id| {
                                                                if let Some(uuid_str) =
                                                                    id.strip_prefix("profile_")
                                                                {
                                                                    Uuid::parse_str(uuid_str).ok()
                                                                } else if let Some(uuid_str) =
                                                                    id.strip_prefix("conn_folder_")
                                                                {
                                                                    Uuid::parse_str(uuid_str).ok()
                                                                } else {
                                                                    None
                                                                }
                                                            })
                                                            .collect();

                                                    let total_count = 1 + additional_nodes.len();
                                                    let preview_label = if total_count > 1 {
                                                        format!(
                                                            "{} (+{} more)",
                                                            drag_label,
                                                            total_count - 1
                                                        )
                                                    } else {
                                                        drag_label
                                                    };

                                                    el.on_drag(
                                                        SidebarDragState {
                                                            node_id,
                                                            additional_nodes,
                                                            is_folder,
                                                            label: preview_label,
                                                        },
                                                        |state, _, _, cx| {
                                                            cx.new(|_| DragPreview {
                                                                label: state.label.clone(),
                                                            })
                                                        },
                                                    )
                                                } else {
                                                    el
                                                }
                                            },
                                        )
                                        // Drop indicator for "After" position
                                        .when(
                                            matches!(
                                                node_kind,
                                                TreeNodeKind::Profile
                                                    | TreeNodeKind::ConnectionFolder
                                            ),
                                            |el| {
                                                let is_drop_after = current_drop_target
                                                    .as_ref()
                                                    .map(|t| {
                                                        t.item_id == item_id.as_ref()
                                                            && t.position == DropPosition::After
                                                    })
                                                    .unwrap_or(false);

                                                if is_drop_after {
                                                    el.border_b_2()
                                                        .border_color(drop_indicator_color)
                                                } else {
                                                    el
                                                }
                                            },
                                        )
                                        // Profile drop handling (insert after)
                                        .when(node_kind == TreeNodeKind::Profile, |el| {
                                            let item_id_for_drop = item_id.to_string();
                                            let item_id_for_move = item_id.to_string();
                                            let sidebar_for_drop = sidebar_entity.clone();
                                            let sidebar_for_move = sidebar_entity.clone();
                                            let item_ix = ix;

                                            el.drag_over::<SidebarDragState>(
                                                move |style, state, _, cx| {
                                                    // Parse profile_id from item_id
                                                    let profile_id = item_id_for_move
                                                        .strip_prefix("profile_")
                                                        .and_then(|s| Uuid::parse_str(s).ok());
                                                    // Don't allow dropping on self
                                                    if profile_id.is_some_and(|_| {
                                                        state.node_id
                                                            != profile_id.unwrap_or(state.node_id)
                                                    }) {
                                                        sidebar_for_move.update(cx, |this, cx| {
                                                            // Clear folder hover (moved away from folder)
                                                            this.clear_drag_hover_folder(cx);
                                                            this.set_drop_target(
                                                                item_id_for_move.clone(),
                                                                DropPosition::After,
                                                                cx,
                                                            );
                                                            // Check for auto-scroll
                                                            this.check_auto_scroll(item_ix, cx);
                                                        });
                                                    }
                                                    style
                                                },
                                            )
                                            .on_drop(
                                                move |state: &SidebarDragState, _, cx| {
                                                    sidebar_for_drop.update(cx, |this, cx| {
                                                        this.stop_auto_scroll(cx);
                                                        this.clear_drag_hover_folder(cx);
                                                        this.set_drop_target(
                                                            item_id_for_drop.clone(),
                                                            DropPosition::After,
                                                            cx,
                                                        );
                                                        this.handle_drop_with_position(state, cx);
                                                    });
                                                },
                                            )
                                        })
                                        // Folder drop handling (insert into)
                                        .when(node_kind == TreeNodeKind::ConnectionFolder, |el| {
                                            let item_id_for_drop = item_id.to_string();
                                            let item_id_for_move = item_id.to_string();
                                            let sidebar_for_drop = sidebar_entity.clone();
                                            let sidebar_for_move = sidebar_entity.clone();
                                            let drop_target_bg = theme.drop_target;
                                            let item_ix = ix;

                                            if let Some(folder_id) = item_id
                                                .strip_prefix("conn_folder_")
                                                .and_then(|s| Uuid::parse_str(s).ok())
                                            {
                                                el.drag_over::<SidebarDragState>(
                                                    move |style, state, _, cx| {
                                                        if state.node_id != folder_id {
                                                            sidebar_for_move.update(
                                                                cx,
                                                                |this, cx| {
                                                                    this.set_drop_target(
                                                                        item_id_for_move.clone(),
                                                                        DropPosition::Into,
                                                                        cx,
                                                                    );
                                                                    // Start auto-expand timer
                                                                    this.start_drag_hover_folder(
                                                                        folder_id, cx,
                                                                    );
                                                                    // Check for auto-scroll
                                                                    this.check_auto_scroll(
                                                                        item_ix, cx,
                                                                    );
                                                                },
                                                            );
                                                            style.bg(drop_target_bg)
                                                        } else {
                                                            style
                                                        }
                                                    },
                                                )
                                                .on_drop(move |state: &SidebarDragState, _, cx| {
                                                    sidebar_for_drop.update(cx, |this, cx| {
                                                        this.stop_auto_scroll(cx);
                                                        this.clear_drag_hover_folder(cx);
                                                        this.set_drop_target(
                                                            item_id_for_drop.clone(),
                                                            DropPosition::Into,
                                                            cx,
                                                        );
                                                        this.handle_drop_with_position(state, cx);
                                                    });
                                                })
                                            } else {
                                                el
                                            }
                                        })
                                        // Menu button for items that have context menus
                                        .when(
                                            matches!(
                                                node_kind,
                                                TreeNodeKind::Profile
                                                    | TreeNodeKind::ConnectionFolder
                                                    | TreeNodeKind::Table
                                                    | TreeNodeKind::View
                                                    | TreeNodeKind::Database
                                                    | TreeNodeKind::Index
                                                    | TreeNodeKind::SchemaIndex
                                                    | TreeNodeKind::ForeignKey
                                                    | TreeNodeKind::SchemaForeignKey
                                                    | TreeNodeKind::CustomType
                                            ),
                                            |el| {
                                                let sidebar_for_menu = sidebar_entity.clone();
                                                let item_id_for_menu = item_id.clone();
                                                let hover_bg = theme.secondary;
                                                let has_menu = node_kind != TreeNodeKind::Database
                                                    || !is_active_database;

                                                el.when(has_menu, |el| {
                                                    el.child(
                                                        div()
                                                            .id(SharedString::from(format!(
                                                                "menu-btn-{}",
                                                                item_id_for_menu
                                                            )))
                                                            .flex_shrink_0()
                                                            .ml_auto()
                                                            .px_1()
                                                            .rounded(Radii::SM)
                                                            .cursor_pointer()
                                                            .hover(move |d| d.bg(hover_bg))
                                                            .on_mouse_down(
                                                                MouseButton::Left,
                                                                |_, _, cx| {
                                                                    cx.stop_propagation();
                                                                },
                                                            )
                                                            .on_click({
                                                                let sidebar = sidebar_for_menu.clone();
                                                                let item_id = item_id_for_menu.clone();
                                                                move |event, _, cx| {
                                                                    cx.stop_propagation();
                                                                    let position = event.position();
                                                                    sidebar.update(cx, |this, cx| {
                                                                        cx.emit(
                                                                            SidebarEvent::RequestFocus,
                                                                        );
                                                                        this.open_menu_for_item(
                                                                            &item_id, position, cx,
                                                                        );
                                                                    });
                                                                }
                                                            })
                                                            .child("⋯"),
                                                    )
                                                })
                                            },
                                        )
                                        // Right-click context menu
                                        .when(
                                            matches!(
                                                node_kind,
                                                TreeNodeKind::Profile
                                                    | TreeNodeKind::ConnectionFolder
                                                    | TreeNodeKind::Table
                                                    | TreeNodeKind::View
                                                    | TreeNodeKind::Database
                                                    | TreeNodeKind::Index
                                                    | TreeNodeKind::SchemaIndex
                                                    | TreeNodeKind::ForeignKey
                                                    | TreeNodeKind::SchemaForeignKey
                                                    | TreeNodeKind::CustomType
                                            ),
                                            |el| {
                                                let sidebar_for_ctx = sidebar_entity.clone();
                                                let item_id_for_ctx = item_id.clone();

                                                el.on_mouse_down(
                                                    MouseButton::Right,
                                                    move |event, _, cx| {
                                                        cx.stop_propagation();
                                                        let position = event.position;
                                                        sidebar_for_ctx.update(cx, |this, cx| {
                                                            cx.emit(SidebarEvent::RequestFocus);
                                                            this.open_menu_for_item(
                                                                &item_id_for_ctx,
                                                                position,
                                                                cx,
                                                            );
                                                        });
                                                    },
                                                )
                                            },
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
                                    let with_ctrl =
                                        event.modifiers().platform || event.modifiers().control;
                                    sidebar.update(cx, |this, cx| {
                                        this.handle_item_click(
                                            &item_id_for_click,
                                            click_count,
                                            with_ctrl,
                                            cx,
                                        );
                                    });
                                });
                            }

                            let is_other_folder = is_folder
                                && matches!(
                                    node_kind,
                                    TreeNodeKind::Schema
                                        | TreeNodeKind::TablesFolder
                                        | TreeNodeKind::ViewsFolder
                                        | TreeNodeKind::TypesFolder
                                        | TreeNodeKind::ColumnsFolder
                                        | TreeNodeKind::IndexesFolder
                                        | TreeNodeKind::ForeignKeysFolder
                                        | TreeNodeKind::ConstraintsFolder
                                        | TreeNodeKind::SchemaIndexesFolder
                                        | TreeNodeKind::SchemaForeignKeysFolder
                                        | TreeNodeKind::CustomType
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

                            list_item
                        },
                    ))
            })
            .child(self.render_footer(cx))
            // Add menu dropdown
            .when(self.add_menu_open, |el| {
                let theme = cx.theme();
                let app_state = self.app_state.clone();
                let sidebar_for_folder = cx.entity().clone();
                let sidebar_for_conn = cx.entity().clone();
                let sidebar_for_close = cx.entity().clone();
                let hover_bg = theme.list_active;

                el.child(
                    // Overlay to close on click outside
                    div()
                        .id("add-menu-overlay")
                        .absolute()
                        .inset_0()
                        .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                            sidebar_for_close.update(cx, |this, cx| {
                                this.close_add_menu(cx);
                            });
                        }),
                )
                .child(
                    // Menu dropdown positioned below the + button
                    div()
                        .absolute()
                        .top(Heights::TOOLBAR)
                        .right(Spacing::XS)
                        .bg(theme.sidebar)
                        .border_1()
                        .border_color(theme.border)
                        .rounded(Radii::SM)
                        .py(Spacing::XS)
                        .min_w(px(140.0))
                        .shadow_md()
                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                            cx.stop_propagation();
                        })
                        .child(
                            div()
                                .id("add-folder-option")
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .hover(move |d| d.bg(hover_bg))
                                .on_click(move |_, _, cx| {
                                    sidebar_for_folder.update(cx, |this, cx| {
                                        this.close_add_menu(cx);
                                        this.create_root_folder(cx);
                                    });
                                })
                                .child(
                                    div()
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::SM)
                                        .child(
                                            svg()
                                                .path(AppIcon::Folder.path())
                                                .size_4()
                                                .text_color(theme.muted_foreground),
                                        )
                                        .child("New Folder"),
                                ),
                        )
                        .child(
                            div()
                                .id("add-connection-option")
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .hover(move |d| d.bg(theme.list_active))
                                .on_click(move |_, _, cx| {
                                    sidebar_for_conn.update(cx, |this, cx| {
                                        this.close_add_menu(cx);
                                    });
                                    let app_state = app_state.clone();
                                    cx.open_window(
                                        WindowOptions {
                                            app_id: Some("dbflux".into()),
                                            titlebar: Some(TitlebarOptions {
                                                title: Some("Connection Manager".into()),
                                                ..Default::default()
                                            }),
                                            window_bounds: Some(WindowBounds::Windowed(
                                                Bounds::centered(
                                                    None,
                                                    size(px(600.0), px(550.0)),
                                                    cx,
                                                ),
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
                                .child(
                                    div()
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::SM)
                                        .child(
                                            svg()
                                                .path(AppIcon::Plug.path())
                                                .size_4()
                                                .text_color(theme.muted_foreground),
                                        )
                                        .child("New Connection"),
                                ),
                        ),
                )
            })
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
