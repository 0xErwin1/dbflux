mod code_generation;
mod context_menu;
mod deletion;
mod drag_drop;
mod expansion;
mod operations;
mod render;
mod render_footer;
mod render_overlays;
mod render_tree;
mod selection;
mod table_loading;
mod tree_builder;

use crate::app::{AppState, AppStateChanged, ConnectedProfile};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use crate::ui::windows::connection_manager::ConnectionManagerWindow;
use crate::ui::windows::settings::SettingsWindow;
use dbflux_core::{
    AddEnumValueRequest, AddForeignKeyRequest, CodeGenCapabilities, CodeGenScope, CollectionRef,
    ConnectionTreeNode, ConnectionTreeNodeKind, ConstraintKind, CreateIndexRequest,
    CreateTypeRequest, CustomTypeInfo, CustomTypeKind, DatabaseCategory, DropForeignKeyRequest,
    DropIndexRequest, DropTypeRequest, ReindexRequest, SchemaCacheKey, SchemaForeignKeyInfo,
    SchemaIndexInfo, SchemaLoadingStrategy, SchemaNodeId, SchemaNodeKind, SchemaSnapshot,
    TableInfo, TableRef, TaskKind, TypeDefinition, ViewInfo,
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
    OpenCollection {
        profile_id: Uuid,
        collection: CollectionRef,
    },
    OpenKeyValueDatabase {
        profile_id: Uuid,
        database: String,
    },
    /// Request to show SQL preview modal
    RequestSqlPreview {
        profile_id: Uuid,
        table_info: TableInfo,
        generation_type: crate::ui::sql_preview_modal::SqlGenerationType,
    },
}

/// Sentinel value for IDs that don't correspond to schema tree nodes
/// (e.g., UI element IDs like "settings-btn" or "row-0").
const NODE_KIND_NONE: SchemaNodeKind = SchemaNodeKind::Placeholder;

/// Parse a tree item ID string into its typed `SchemaNodeKind`.
///
/// Returns `NODE_KIND_NONE` for IDs that can't be parsed (UI element IDs etc.).
/// This avoids pervasive `Option` unwrapping at every call site.
fn parse_node_kind(id: &str) -> SchemaNodeKind {
    id.parse::<SchemaNodeId>()
        .ok()
        .map(|n| n.kind())
        .unwrap_or(NODE_KIND_NONE)
}

/// Parse a tree item ID string into its full typed `SchemaNodeId`.
fn parse_node_id(id: &str) -> Option<SchemaNodeId> {
    id.parse().ok()
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
    Refresh,
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
            Self::Refresh => Some(AppIcon::RefreshCcw),
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

impl ItemIdParts {
    fn from_node_id(node_id: &SchemaNodeId) -> Option<Self> {
        match node_id {
            SchemaNodeId::Table {
                profile_id,
                schema,
                name,
            }
            | SchemaNodeId::View {
                profile_id,
                schema,
                name,
            } => Some(Self {
                profile_id: *profile_id,
                schema_name: schema.clone(),
                object_name: name.clone(),
            }),
            _ => None,
        }
    }
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

use crate::ui::toast::PendingToast;

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

    pub fn execute(&mut self, cx: &mut Context<Self>) {
        let entry = self.tree_state.read(cx).selected_entry().cloned();
        if let Some(entry) = entry {
            let item_id = entry.item().id.to_string();
            self.execute_item(&item_id, cx);
        }
    }

    fn execute_item(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(node_id) = parse_node_id(item_id) else {
            return;
        };

        match node_id {
            SchemaNodeId::Table { .. } | SchemaNodeId::View { .. } => {
                self.browse_table(item_id, cx);
            }
            SchemaNodeId::Collection { .. } => {
                self.browse_collection(item_id, cx);
            }
            SchemaNodeId::Profile { profile_id } => {
                let is_connected = self
                    .app_state
                    .read(cx)
                    .connections()
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
            SchemaNodeId::Database { .. } => {
                self.handle_database_click(item_id, cx);

                if let Some(SchemaNodeId::Database {
                    profile_id,
                    name: database,
                }) = parse_node_id(item_id)
                    && self.profile_category(profile_id, cx) == Some(DatabaseCategory::KeyValue)
                {
                    cx.emit(SidebarEvent::OpenKeyValueDatabase {
                        profile_id,
                        database,
                    });
                }
            }
            _ => {}
        }
    }

    fn profile_category(&self, profile_id: Uuid, cx: &App) -> Option<DatabaseCategory> {
        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.connection.metadata().category)
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

        let node_kind = parse_node_kind(item_id);

        if click_count == 2 {
            let is_key_value_db = matches!(parse_node_id(item_id), Some(SchemaNodeId::Database { profile_id, .. }) if self.profile_category(profile_id, cx) == Some(DatabaseCategory::KeyValue));

            if is_key_value_db {
                self.toggle_item_expansion(item_id, cx);
                self.execute_item(item_id, cx);
            } else if node_kind.is_expandable_folder() {
                self.toggle_item_expansion(item_id, cx);
            } else {
                self.execute_item(item_id, cx);
            }
        }

        cx.notify();
    }

    fn browse_table(&mut self, item_id: &str, cx: &mut Context<Self>) {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::Table {
                profile_id,
                schema,
                name,
            })
            | Some(SchemaNodeId::View {
                profile_id,
                schema,
                name,
            }) => {
                let table = TableRef::with_schema(&schema, &name);
                cx.emit(SidebarEvent::OpenTable { profile_id, table });
            }
            _ => {}
        }
    }

    fn browse_collection(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if let Some(SchemaNodeId::Collection {
            profile_id,
            database,
            name,
        }) = parse_node_id(item_id)
        {
            let collection = CollectionRef::new(&database, &name);
            cx.emit(SidebarEvent::OpenCollection {
                profile_id,
                collection,
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
}

#[cfg(test)]
mod tests {
    use super::{ItemIdParts, NODE_KIND_NONE, parse_node_kind};
    use dbflux_core::{SchemaNodeId, SchemaNodeKind};
    use uuid::Uuid;

    fn test_uuid() -> Uuid {
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    #[test]
    fn table_id_roundtrip() {
        let id = SchemaNodeId::Table {
            profile_id: test_uuid(),
            schema: "public".into(),
            name: "users".into(),
        };
        let s = id.to_string();
        let parsed: SchemaNodeId = s.parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn view_id_roundtrip() {
        let id = SchemaNodeId::View {
            profile_id: test_uuid(),
            schema: "analytics".into(),
            name: "monthly_stats".into(),
        };
        let s = id.to_string();
        let parsed: SchemaNodeId = s.parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn table_with_special_chars_in_name() {
        let id = SchemaNodeId::Table {
            profile_id: test_uuid(),
            schema: "public".into(),
            name: "user_accounts_archive".into(),
        };
        let s = id.to_string();
        let parsed: SchemaNodeId = s.parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn collection_id_roundtrip() {
        let id = SchemaNodeId::Collection {
            profile_id: test_uuid(),
            database: "mydb".into(),
            name: "orders".into(),
        };
        let s = id.to_string();
        let parsed: SchemaNodeId = s.parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn parse_node_kind_returns_correct_kind() {
        let table_id = SchemaNodeId::Table {
            profile_id: test_uuid(),
            schema: "public".into(),
            name: "users".into(),
        }
        .to_string();

        assert_eq!(parse_node_kind(&table_id), SchemaNodeKind::Table);
    }

    #[test]
    fn parse_node_kind_returns_placeholder_for_invalid_id() {
        assert_eq!(parse_node_kind("garbage"), NODE_KIND_NONE);
    }

    #[test]
    fn item_id_parts_from_table_node() {
        let id = SchemaNodeId::Table {
            profile_id: test_uuid(),
            schema: "public".into(),
            name: "users".into(),
        };
        let parts = ItemIdParts::from_node_id(&id).unwrap();
        assert_eq!(parts.profile_id, test_uuid());
        assert_eq!(parts.schema_name, "public");
        assert_eq!(parts.object_name, "users");
    }

    #[test]
    fn item_id_parts_from_view_node() {
        let id = SchemaNodeId::View {
            profile_id: test_uuid(),
            schema: "analytics".into(),
            name: "monthly_stats".into(),
        };
        let parts = ItemIdParts::from_node_id(&id).unwrap();
        assert_eq!(parts.profile_id, test_uuid());
        assert_eq!(parts.schema_name, "analytics");
        assert_eq!(parts.object_name, "monthly_stats");
    }

    #[test]
    fn item_id_parts_from_non_table_returns_none() {
        let id = SchemaNodeId::Database {
            profile_id: test_uuid(),
            name: "mydb".into(),
        };
        assert!(ItemIdParts::from_node_id(&id).is_none());
    }

    #[test]
    fn database_id_roundtrip() {
        let id = SchemaNodeId::Database {
            profile_id: test_uuid(),
            name: "my_database".into(),
        };
        let s = id.to_string();
        let parsed: SchemaNodeId = s.parse().unwrap();
        assert_eq!(parsed, id);
    }
}
