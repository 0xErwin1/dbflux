//! Source & Target phase of the migration wizard: two lazy-loaded object-tree
//! pickers side by side. The left tree browses the source connection
//! (connection → database → schema → table) with checkable table leaves — it
//! opens pre-checked with the tables the sidebar passed and stays fully
//! editable (uncheck, check others, browse other databases). The right tree
//! lists only connected, transfer-compatible targets (connection → database)
//! and captures which target *container* the migration loads into; the
//! per-table target name/mode is chosen later in the mapping grid, not here
//! (design ADR #5).
//!
//! `TreeNav` is reused as the pure nav model (rows / expand / cursor). The
//! checkbox state and per-node lazy-load state live in the wizard-owned
//! [`TreeModel`] (design ADR #3/#4) — `TreeNav` renders nothing itself, so the
//! checkbox glyph is drawn here over `TreeModel::is_checked`, and an
//! unexpanded/loading/failed branch surfaces a synthetic child row
//! (`Loading…` / `Retry`) so the branch stays expandable and its state is
//! visible. The wizard entity mounts this phase as the first step of the flow.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use dbflux_components::components::tree_nav::{
    TreeNav, TreeNavAction, TreeNavNode, render_gutter, tree_line_color,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Spacing};
use dbflux_core::{Connection, TableRef, transfer_compatible};
use dbflux_ui_base::app_state_entity::AppStateEntity;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

use crate::migrate_wizard::tree_model::{
    NodeLoad, TreeModel, TreePayload, connection_node_id, database_node_id, schema_node_id,
    table_node_id,
};

const ROW_HEIGHT: Pixels = Heights::ROW_COMPACT;
const INDENT_PX: f32 = 14.0;

/// Which of the two trees an operation targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeSide {
    Source,
    Target,
}

/// Emitted whenever the checked source tables or the chosen target container
/// change, so the host can re-evaluate the phase-advance guard.
#[derive(Debug, Clone)]
pub struct SourceTargetChanged;

/// The container the migration loads into: a connected, transfer-compatible
/// profile plus the specific database. Feeds `MigrationOptions::target_database`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSelection {
    pub profile_id: Uuid,
    pub database: String,
}

/// One table under a database/schema, decoupled from `TableInfo` so tree
/// construction is pure and testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEntry {
    pub schema: Option<String>,
    pub name: String,
}

/// A schema group holding its tables (schema-based drivers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaNode {
    pub name: String,
    pub tables: Vec<TableEntry>,
}

/// A database node. `schemas` is used by schema-based drivers; `tables` holds
/// schemaless tables directly under the database. Both empty means the node's
/// contents have not been loaded yet (or the database is genuinely empty).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DbNode {
    pub name: String,
    pub schemas: Vec<SchemaNode>,
    pub tables: Vec<TableEntry>,
}

/// A connection root and the databases known under it so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnRoot {
    pub profile_id: Uuid,
    pub label: String,
    pub databases: Vec<DbNode>,
}

fn status_child_id(parent: &SharedString) -> SharedString {
    SharedString::from(format!("{parent}::__status"))
}

fn retry_child_id(parent: &SharedString) -> SharedString {
    SharedString::from(format!("{parent}::__retry"))
}

fn is_status_id(id: &str) -> bool {
    id.ends_with("::__status")
}

fn is_retry_id(id: &str) -> bool {
    id.ends_with("::__retry")
}

/// The real node id an on-screen `Loading…`/`Retry` child stands in for.
fn parent_of_synthetic(id: &str) -> Option<&str> {
    id.strip_suffix("::__retry")
        .or_else(|| id.strip_suffix("::__status"))
}

/// The synthetic child rendered under an expandable branch whose contents are
/// not yet available, so the branch stays expandable (a childless `TreeNav`
/// group is not) and its load state is visible. `Loaded` needs no placeholder.
fn status_child(parent: &SharedString, load: &NodeLoad) -> Option<TreeNavNode> {
    match load {
        NodeLoad::Loaded => None,
        NodeLoad::NotLoaded | NodeLoad::Loading => {
            let mut node =
                TreeNavNode::leaf(status_child_id(parent), "Loading…", Some(AppIcon::Loader));
            node.selectable = false;
            Some(node)
        }
        NodeLoad::Failed(error) => {
            let mut node = TreeNavNode::leaf(
                retry_child_id(parent),
                SharedString::from(format!("Retry — {error}")),
                Some(AppIcon::RotateCcw),
            );
            node.selectable = true;
            Some(node)
        }
    }
}

/// Builds the full `TreeNav` node list for one side from the known structure
/// and the load/payload state, registering every real node's payload so click
/// and key handlers can act on ids without parsing them. Pure over its inputs.
pub fn build_side_nodes(
    side: TreeSide,
    roots: &[ConnRoot],
    model: &mut TreeModel,
) -> Vec<TreeNavNode> {
    roots
        .iter()
        .map(|root| build_connection_node(side, root, model))
        .collect()
}

fn build_connection_node(side: TreeSide, root: &ConnRoot, model: &mut TreeModel) -> TreeNavNode {
    let id = connection_node_id(root.profile_id);
    model.insert_payload(id.clone(), TreePayload::Connection(root.profile_id));

    let mut children: Vec<TreeNavNode> = root
        .databases
        .iter()
        .map(|db| build_database_node(side, root.profile_id, db, model))
        .collect();

    if children.is_empty() {
        children.extend(status_child(&id, &model.load(&id)));
    }

    TreeNavNode::group(id, root.label.clone(), Some(AppIcon::Server), children)
}

fn build_database_node(
    side: TreeSide,
    profile_id: Uuid,
    db: &DbNode,
    model: &mut TreeModel,
) -> TreeNavNode {
    let id = database_node_id(profile_id, &db.name);
    model.insert_payload(
        id.clone(),
        TreePayload::Database {
            profile_id,
            database: db.name.clone(),
        },
    );

    if side == TreeSide::Target {
        // The target tree stops at the database — that is the container the
        // migration loads into; per-table mapping is the grid's job.
        return TreeNavNode::leaf(id, db.name.clone(), Some(AppIcon::Database));
    }

    let mut children = build_database_children(profile_id, db, model);
    if children.is_empty() {
        children.extend(status_child(&id, &model.load(&id)));
    }

    TreeNavNode::group(id, db.name.clone(), Some(AppIcon::Database), children)
}

fn build_database_children(
    profile_id: Uuid,
    db: &DbNode,
    model: &mut TreeModel,
) -> Vec<TreeNavNode> {
    if !db.schemas.is_empty() {
        return db
            .schemas
            .iter()
            .map(|schema| build_schema_node(profile_id, &db.name, schema, model))
            .collect();
    }

    db.tables
        .iter()
        .map(|table| build_table_leaf(profile_id, &db.name, table, model))
        .collect()
}

fn build_schema_node(
    profile_id: Uuid,
    database: &str,
    schema: &SchemaNode,
    model: &mut TreeModel,
) -> TreeNavNode {
    let id = schema_node_id(profile_id, database, &schema.name);
    model.insert_payload(
        id.clone(),
        TreePayload::Schema {
            profile_id,
            database: database.to_string(),
            schema: schema.name.clone(),
        },
    );

    let children: Vec<TreeNavNode> = schema
        .tables
        .iter()
        .map(|table| build_table_leaf(profile_id, database, table, model))
        .collect();

    TreeNavNode::group(id, schema.name.clone(), Some(AppIcon::Folder), children)
}

fn build_table_leaf(
    profile_id: Uuid,
    database: &str,
    table: &TableEntry,
    model: &mut TreeModel,
) -> TreeNavNode {
    let id = table_node_id(profile_id, database, table.schema.as_deref(), &table.name);
    model.insert_payload(
        id.clone(),
        TreePayload::Table {
            profile_id,
            database: database.to_string(),
            schema: table.schema.clone(),
            table: TableRef {
                schema: table.schema.clone(),
                name: table.name.clone(),
            },
        },
    );

    TreeNavNode::leaf(id, table.name.clone(), Some(AppIcon::Table))
}

/// Groups a database's tables into schemaless (`None` key) and per-schema
/// buckets, preserving encounter order within each bucket — the pure core of
/// turning a freshly fetched table list into [`DbNode`] children.
fn split_tables_by_schema(tables: Vec<TableEntry>) -> (Vec<TableEntry>, Vec<SchemaNode>) {
    let mut schemaless = Vec::new();
    let mut by_schema: BTreeMap<String, Vec<TableEntry>> = BTreeMap::new();

    for table in tables {
        match table.schema.clone() {
            None => schemaless.push(table),
            Some(schema) => by_schema.entry(schema).or_default().push(table),
        }
    }

    let schemas = by_schema
        .into_iter()
        .map(|(name, tables)| SchemaNode { name, tables })
        .collect();

    (schemaless, schemas)
}

/// Whether a source-side node may be checked: a migration reads from exactly
/// one database, so only a table leaf that lives in `source_database` is
/// selectable. Gating both the checkbox and the toggle on this makes it
/// impossible to check a same-named table in another browsed database — which
/// would otherwise be silently migrated in place of the intended one.
fn is_source_table_checkable(payload: Option<&TreePayload>, source_database: &str) -> bool {
    matches!(
        payload,
        Some(TreePayload::Table { database, .. }) if database == source_database
    )
}

/// Resolves the wizard-owned checked set to `TableRef`s, keeping only tables
/// that live in `source_database`. Any stray check from another browsed
/// database is dropped, so the returned tables always resolve against the
/// single source the plan is built for.
fn checked_tables_in_database(model: &TreeModel, source_database: &str) -> Vec<TableRef> {
    model
        .checked_ids()
        .filter_map(|id| match model.payload(id) {
            Some(TreePayload::Table {
                database, table, ..
            }) if database == source_database => Some(table.clone()),
            _ => None,
        })
        .collect()
}

/// Per-side runtime state: the known structure, the payload/load/checked
/// model, and the `TreeNav` nav state built from them.
struct SideState {
    roots: Vec<ConnRoot>,
    model: TreeModel,
    tree: TreeNav,
}

pub struct SourceTargetPhase {
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    source_profile_id: Uuid,
    /// The single database the migration reads from. A migration has exactly
    /// one source database, so only tables that live in it are checkable —
    /// checking a same-named table in another browsed database would silently
    /// migrate the wrong table (the plan resolves one source database only).
    source_database: String,
    source: SideState,
    target: SideState,
    active_side: TreeSide,
    target_selection: Option<TargetSelection>,
    error: Option<String>,
}

impl EventEmitter<SourceTargetChanged> for SourceTargetPhase {}

impl SourceTargetPhase {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        source_profile_id: Uuid,
        source_database: Option<String>,
        source_tables: Vec<TableRef>,
        cx: &mut Context<Self>,
    ) -> Self {
        let (source_roots, resolved_database) =
            Self::source_roots_from_state(&app_state, source_profile_id, source_database, cx);
        let target_roots = Self::target_roots_from_state(&app_state, source_profile_id, cx);

        let mut source_model = TreeModel::new();
        let seed = source_model.seed_source_selection(
            source_profile_id,
            &resolved_database,
            &source_tables,
        );
        let source_nodes = build_side_nodes(TreeSide::Source, &source_roots, &mut source_model);
        let mut source_tree = TreeNav::new(source_nodes, seed.expand);
        if let Some(cursor) = seed.cursor {
            source_tree.select_by_id(&cursor);
        }

        let mut target_model = TreeModel::new();
        let target_nodes = build_side_nodes(TreeSide::Target, &target_roots, &mut target_model);
        let target_tree = TreeNav::new(target_nodes, HashSet::new());

        Self {
            app_state,
            focus_handle: cx.focus_handle(),
            source_profile_id,
            source_database: resolved_database,
            source: SideState {
                roots: source_roots,
                model: source_model,
                tree: source_tree,
            },
            target: SideState {
                roots: target_roots,
                model: target_model,
                tree: target_tree,
            },
            active_side: TreeSide::Source,
            target_selection: None,
            error: None,
        }
    }

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// The tables the user has checked in the source tree, resolved from the
    /// wizard-owned checked set back to `TableRef`s via the payload map.
    /// Constrained to the single [`source_database`](Self::source_database):
    /// a migration reads from exactly one database, so a stray check in
    /// another browsed database is never returned as a source table.
    pub fn checked_source_tables(&self) -> Vec<TableRef> {
        checked_tables_in_database(&self.source.model, &self.source_database)
    }

    /// The single database this migration reads from. Downstream plan
    /// assembly must use exactly this database so that every table from
    /// [`checked_source_tables`](Self::checked_source_tables) resolves against
    /// the same source — see the cross-database check guard in `on_select`.
    pub fn source_database(&self) -> &str {
        &self.source_database
    }

    pub fn target_profile_id(&self) -> Option<Uuid> {
        self.target_selection.as_ref().map(|t| t.profile_id)
    }

    pub fn target_database(&self) -> Option<String> {
        self.target_selection.as_ref().map(|t| t.database.clone())
    }

    /// Whether the phase's advance guard is satisfied: at least one checked
    /// source table and a chosen target container. Transfer compatibility is
    /// already guaranteed — only compatible targets appear in the tree.
    pub fn is_ready(&self) -> bool {
        self.source.model.checked_count() > 0 && self.target_selection.is_some()
    }

    fn source_roots_from_state(
        app_state: &Entity<AppStateEntity>,
        source_profile_id: Uuid,
        source_database: Option<String>,
        cx: &App,
    ) -> (Vec<ConnRoot>, String) {
        let state = app_state.read(cx);
        let Some(connected) = state.connections().get(&source_profile_id) else {
            return (Vec::new(), source_database.unwrap_or_default());
        };

        let relational = connected.schema.as_ref().and_then(relational_schema);
        let current_database = source_database
            .or_else(|| relational.and_then(|r| r.current_database.clone()))
            .or_else(|| connected.connection.active_database())
            .unwrap_or_default();

        let mut databases = Vec::new();
        if let Some(relational) = relational {
            databases.push(current_database_node(&current_database, relational));

            for database in &relational.databases {
                if database.name != current_database {
                    databases.push(DbNode {
                        name: database.name.clone(),
                        ..Default::default()
                    });
                }
            }
        } else if !current_database.is_empty() {
            databases.push(DbNode {
                name: current_database.clone(),
                ..Default::default()
            });
        }

        let root = ConnRoot {
            profile_id: source_profile_id,
            label: connected.profile.name.clone(),
            databases,
        };
        (vec![root], current_database)
    }

    fn target_roots_from_state(
        app_state: &Entity<AppStateEntity>,
        source_profile_id: Uuid,
        cx: &App,
    ) -> Vec<ConnRoot> {
        let state = app_state.read(cx);
        let Some(source_connected) = state.connections().get(&source_profile_id) else {
            return Vec::new();
        };
        let source_metadata = source_connected.connection.metadata();

        let mut roots: Vec<ConnRoot> = state
            .connections()
            .iter()
            .filter(|(_, connected)| {
                transfer_compatible(source_metadata, connected.connection.metadata())
            })
            .map(|(profile_id, connected)| {
                let databases = connected
                    .schema
                    .as_ref()
                    .and_then(relational_schema)
                    .map(|relational| {
                        relational
                            .databases
                            .iter()
                            .map(|database| DbNode {
                                name: database.name.clone(),
                                ..Default::default()
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                ConnRoot {
                    profile_id: *profile_id,
                    label: connected.profile.name.clone(),
                    databases,
                }
            })
            .collect();

        roots.sort_by(|a, b| a.label.cmp(&b.label));
        roots
    }

    fn side(&self, side: TreeSide) -> &SideState {
        match side {
            TreeSide::Source => &self.source,
            TreeSide::Target => &self.target,
        }
    }

    fn side_mut(&mut self, side: TreeSide) -> &mut SideState {
        match side {
            TreeSide::Source => &mut self.source,
            TreeSide::Target => &mut self.target,
        }
    }

    fn rebuild_side(&mut self, side: TreeSide) {
        let state = self.side_mut(side);
        let nodes = build_side_nodes(side, &state.roots, &mut state.model);
        state.tree.set_nodes(nodes);
    }

    fn activate_current(&mut self, side: TreeSide, cx: &mut Context<Self>) {
        let action = self.side_mut(side).tree.activate();
        self.handle_action(side, action, cx);
    }

    fn handle_action(&mut self, side: TreeSide, action: TreeNavAction, cx: &mut Context<Self>) {
        match action {
            TreeNavAction::Selected(id) => self.on_select(side, id, cx),
            TreeNavAction::Toggled { id, expanded } => self.on_toggle(side, id, expanded, cx),
            TreeNavAction::None => {}
        }
    }

    fn on_select(&mut self, side: TreeSide, id: SharedString, cx: &mut Context<Self>) {
        if is_retry_id(&id) {
            self.retry(side, &id, cx);
            return;
        }

        match self.side(side).model.payload(&id).cloned() {
            Some(TreePayload::Table { database, .. }) if side == TreeSide::Source => {
                if database == self.source_database {
                    self.source.model.toggle_checked(&id);
                    self.error = None;
                    cx.emit(SourceTargetChanged);
                    cx.notify();
                } else {
                    self.error = Some(format!(
                        "A migration has a single source database. Only tables in \
                         '{}' can be selected — tables in '{database}' can't be mixed in.",
                        self.source_database
                    ));
                    cx.notify();
                }
            }
            Some(TreePayload::Database {
                profile_id,
                database,
            }) if side == TreeSide::Target => {
                self.target_selection = Some(TargetSelection {
                    profile_id,
                    database,
                });
                cx.emit(SourceTargetChanged);
                cx.notify();
            }
            _ => {}
        }
    }

    fn on_toggle(
        &mut self,
        side: TreeSide,
        id: SharedString,
        expanded: bool,
        cx: &mut Context<Self>,
    ) {
        if expanded && self.side(side).model.load(&id) == NodeLoad::NotLoaded {
            self.start_fetch_for(side, &id, cx);
        }
        cx.notify();
    }

    fn retry(&mut self, side: TreeSide, retry_id: &str, cx: &mut Context<Self>) {
        let Some(parent) = parent_of_synthetic(retry_id) else {
            return;
        };
        let parent = SharedString::from(parent.to_string());
        self.start_fetch_for(side, &parent, cx);
    }

    /// Dispatches the correct lazy fetch for a branch based on its payload:
    /// a source database loads its schemas/tables; a target connection loads
    /// its databases. Other payloads are already fully materialized.
    fn start_fetch_for(&mut self, side: TreeSide, id: &SharedString, cx: &mut Context<Self>) {
        match self.side(side).model.payload(id).cloned() {
            Some(TreePayload::Database { database, .. }) if side == TreeSide::Source => {
                self.fetch_source_database(id.clone(), database, cx);
            }
            Some(TreePayload::Connection(profile_id)) if side == TreeSide::Target => {
                self.fetch_target_databases(id.clone(), profile_id, cx);
            }
            _ => {}
        }
    }

    fn fetch_source_database(
        &mut self,
        node_id: SharedString,
        database: String,
        cx: &mut Context<Self>,
    ) {
        let Some(connection) = self.resolve_source_connection(&database, cx) else {
            self.source.model.set_load(
                node_id,
                NodeLoad::Failed("Source connection is gone".to_string()),
            );
            self.rebuild_side(TreeSide::Source);
            cx.notify();
            return;
        };

        self.source
            .model
            .set_load(node_id.clone(), NodeLoad::Loading);
        self.rebuild_side(TreeSide::Source);
        cx.notify();

        let fetch_database = database.clone();
        cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move { connection.schema_for_database(&fetch_database) })
                .await;

            this.update(cx, |this, cx| {
                match result {
                    Ok(info) => {
                        let tables = info
                            .tables
                            .into_iter()
                            .map(|table| TableEntry {
                                schema: table.schema,
                                name: table.name,
                            })
                            .collect();
                        this.populate_source_database(&database, tables);
                        this.source
                            .model
                            .set_load(node_id.clone(), NodeLoad::Loaded);
                    }
                    Err(error) => {
                        this.source
                            .model
                            .set_load(node_id.clone(), NodeLoad::Failed(error.to_string()));
                    }
                }
                this.rebuild_side(TreeSide::Source);
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn fetch_target_databases(
        &mut self,
        node_id: SharedString,
        profile_id: Uuid,
        cx: &mut Context<Self>,
    ) {
        let Some(connection) = self.resolve_target_connection(profile_id, cx) else {
            self.target.model.set_load(
                node_id,
                NodeLoad::Failed("Target connection is gone".to_string()),
            );
            self.rebuild_side(TreeSide::Target);
            cx.notify();
            return;
        };

        self.target
            .model
            .set_load(node_id.clone(), NodeLoad::Loading);
        self.rebuild_side(TreeSide::Target);
        cx.notify();

        cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move { connection.list_databases() })
                .await;

            this.update(cx, |this, cx| {
                match result {
                    Ok(databases) => {
                        let names = databases
                            .into_iter()
                            .map(|database| database.name)
                            .collect();
                        this.populate_target_databases(profile_id, names);
                        this.target
                            .model
                            .set_load(node_id.clone(), NodeLoad::Loaded);
                    }
                    Err(error) => {
                        this.target
                            .model
                            .set_load(node_id.clone(), NodeLoad::Failed(error.to_string()));
                    }
                }
                this.rebuild_side(TreeSide::Target);
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn populate_source_database(&mut self, database: &str, tables: Vec<TableEntry>) {
        let Some(root) = self
            .source
            .roots
            .iter_mut()
            .find(|root| root.profile_id == self.source_profile_id)
        else {
            return;
        };
        let Some(db) = root.databases.iter_mut().find(|db| db.name == database) else {
            return;
        };

        let (schemaless, schemas) = split_tables_by_schema(tables);
        db.tables = schemaless;
        db.schemas = schemas;
    }

    fn populate_target_databases(&mut self, profile_id: Uuid, names: Vec<String>) {
        let Some(root) = self
            .target
            .roots
            .iter_mut()
            .find(|root| root.profile_id == profile_id)
        else {
            return;
        };

        root.databases = names
            .into_iter()
            .map(|name| DbNode {
                name,
                ..Default::default()
            })
            .collect();
    }

    fn resolve_source_connection(&self, database: &str, cx: &App) -> Option<Arc<dyn Connection>> {
        let connected = self
            .app_state
            .read(cx)
            .connections()
            .get(&self.source_profile_id)?;
        Some(connected.connection_for_database(database))
    }

    fn resolve_target_connection(&self, profile_id: Uuid, cx: &App) -> Option<Arc<dyn Connection>> {
        Some(
            self.app_state
                .read(cx)
                .connections()
                .get(&profile_id)?
                .connection
                .clone(),
        )
    }

    fn handle_key_down(
        &mut self,
        event: &KeyDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if event.keystroke.modifiers != Modifiers::none() {
            return;
        }

        let side = self.active_side;
        match event.keystroke.key.as_str() {
            "down" | "j" => {
                self.side_mut(side).tree.move_next();
                cx.notify();
            }
            "up" | "k" => {
                self.side_mut(side).tree.move_prev();
                cx.notify();
            }
            "left" => self.collapse_cursor(side, cx),
            "right" => self.expand_cursor(side, cx),
            "enter" | "space" => self.activate_current(side, cx),
            "tab" => {
                self.active_side = match side {
                    TreeSide::Source => TreeSide::Target,
                    TreeSide::Target => TreeSide::Source,
                };
                cx.notify();
            }
            _ => {}
        }
    }

    fn collapse_cursor(&mut self, side: TreeSide, cx: &mut Context<Self>) {
        let should = self
            .side(side)
            .tree
            .cursor_item()
            .is_some_and(|row| row.has_children && !row.selectable && row.expanded);
        if should {
            self.activate_current(side, cx);
        }
    }

    fn expand_cursor(&mut self, side: TreeSide, cx: &mut Context<Self>) {
        let should = self
            .side(side)
            .tree
            .cursor_item()
            .is_some_and(|row| row.has_children && !row.selectable && !row.expanded);
        if should {
            self.activate_current(side, cx);
        }
    }
}

impl Render for SourceTargetPhase {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .track_focus(&self.focus_handle)
            .key_context("MigrateSourceTarget")
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                this.handle_key_down(event, window, cx);
            }))
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .p(Spacing::MD)
            .size_full()
            .child(
                div()
                    .flex()
                    .flex_row()
                    .gap(Spacing::MD)
                    .flex_1()
                    .min_h(px(0.0))
                    .child(self.render_tree_panel(TreeSide::Source, "Source", cx))
                    .child(self.render_tree_panel(TreeSide::Target, "Target", cx)),
            )
            .when_some(self.error.clone(), |parent, error| {
                parent.child(Text::caption(error))
            })
    }
}

impl SourceTargetPhase {
    fn render_tree_panel(
        &self,
        side: TreeSide,
        title: &str,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let subtitle = match side {
            TreeSide::Source => format!("{} checked", self.source.model.checked_count()),
            TreeSide::Target => self
                .target_selection
                .as_ref()
                .map(|selection| selection.database.clone())
                .unwrap_or_else(|| "No target selected".to_string()),
        };

        div()
            .flex_1()
            .flex()
            .flex_col()
            .min_w(px(0.0))
            .border_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(px(2.0))
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(Text::body(title.to_string()))
                    .child(Text::caption(subtitle)),
            )
            .child(
                div()
                    .id(SharedString::from(format!("migrate-tree-{title}")))
                    .flex_1()
                    .min_h(px(0.0))
                    .overflow_y_scroll()
                    .p(Spacing::XS)
                    .children(self.render_rows(side, &theme, cx)),
            )
    }

    fn render_rows(
        &self,
        side: TreeSide,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let state = self.side(side);
        let cursor = state.tree.cursor();
        let active = self.active_side == side;
        let line_color = tree_line_color(theme);

        state
            .tree
            .rows()
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let is_cursor = active && index == cursor;
                let gutter = render_gutter(
                    row.depth,
                    row.is_last,
                    &row.ancestors_continue,
                    INDENT_PX,
                    ROW_HEIGHT,
                    line_color,
                    false,
                );
                self.render_row(side, row, is_cursor, gutter, theme, cx)
            })
            .collect()
    }

    fn render_row(
        &self,
        side: TreeSide,
        row: &dbflux_components::components::tree_nav::FlatRow,
        is_cursor: bool,
        gutter: AnyElement,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let row_id = row.id.clone();
        let synthetic = is_status_id(&row_id) || is_retry_id(&row_id);
        let is_checkable = side == TreeSide::Source
            && is_source_table_checkable(
                self.side(side).model.payload(&row_id),
                &self.source_database,
            );
        let is_checked = is_checkable && self.side(side).model.is_checked(&row_id);
        let is_target_selected = side == TreeSide::Target
            && self
                .target_selection
                .as_ref()
                .zip(self.side(side).model.payload(&row_id))
                .is_some_and(|(selection, payload)| match payload {
                    TreePayload::Database {
                        profile_id,
                        database,
                    } => selection.profile_id == *profile_id && &selection.database == database,
                    _ => false,
                });

        let text_color = if synthetic {
            theme.muted_foreground
        } else {
            theme.foreground
        };
        let icon_color = if is_target_selected || is_checked {
            theme.primary
        } else {
            theme.muted_foreground
        };

        let mut content = div()
            .flex()
            .items_center()
            .gap(Spacing::XXS)
            .flex_1()
            .min_w(px(0.0))
            .when(is_checkable, |parent| {
                parent.child(checkbox_glyph(is_checked, theme))
            })
            .when_some(row.icon, |parent, icon| {
                parent.child(Icon::new(icon).small().color(icon_color))
            })
            .child(Text::body(row.label.to_string()).color(text_color));

        if is_target_selected {
            content = content
                .child(div().flex_1())
                .child(Icon::new(AppIcon::Check).small().color(theme.primary));
        }

        div()
            .id(row_id.clone())
            .flex()
            .items_center()
            .h(ROW_HEIGHT)
            .px(px(2.0))
            .cursor_pointer()
            .when(is_cursor, |parent| parent.bg(theme.accent))
            .child(gutter)
            .child(content)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _event, _window, cx| {
                    this.active_side = side;
                    this.side_mut(side).tree.select_by_id(&row_id);
                    this.activate_current(side, cx);
                }),
            )
            .into_any_element()
    }
}

/// A small bordered checkbox glyph drawn over the wizard-owned checked set —
/// `TreeNav` holds no checkbox state, so the wizard renders its own.
fn checkbox_glyph(checked: bool, theme: &gpui_component::Theme) -> impl IntoElement {
    div()
        .size(px(14.0))
        .flex()
        .items_center()
        .justify_center()
        .border_1()
        .rounded(px(3.0))
        .border_color(if checked { theme.primary } else { theme.border })
        .when(checked, |parent| parent.bg(theme.primary))
        .when(checked, |parent| {
            parent.child(
                Icon::new(AppIcon::Check)
                    .small()
                    .color(theme.primary_foreground),
            )
        })
}

/// The relational view of a schema snapshot, or `None` for non-relational
/// paradigms (which are not transfer-compatible migration targets anyway).
fn relational_schema(
    snapshot: &dbflux_core::SchemaSnapshot,
) -> Option<&dbflux_core::RelationalSchema> {
    match &snapshot.structure {
        dbflux_core::DataStructure::Relational(relational) => Some(relational),
        _ => None,
    }
}

/// Builds the pre-loaded current-database node from the connection's live
/// schema snapshot (schemas for schema-based drivers, top-level tables for
/// schemaless ones) so the pre-checked source tables are visible immediately.
fn current_database_node(database: &str, relational: &dbflux_core::RelationalSchema) -> DbNode {
    let schemas = relational
        .schemas
        .iter()
        .map(|schema| SchemaNode {
            name: schema.name.clone(),
            tables: schema
                .tables
                .iter()
                .map(|table| TableEntry {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                })
                .collect(),
        })
        .collect();

    let tables = relational
        .tables
        .iter()
        .map(|table| TableEntry {
            schema: table.schema.clone(),
            name: table.name.clone(),
        })
        .collect();

    DbNode {
        name: database.to_string(),
        schemas,
        tables,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConnRoot, DbNode, SchemaNode, TableEntry, TreeSide, build_side_nodes,
        checked_tables_in_database, is_retry_id, is_source_table_checkable, is_status_id,
        parent_of_synthetic, split_tables_by_schema,
    };
    use crate::migrate_wizard::tree_model::{
        NodeLoad, TreeModel, TreePayload, connection_node_id, database_node_id, schema_node_id,
        table_node_id,
    };
    use dbflux_core::TableRef;
    use uuid::Uuid;

    fn uuid(seed: u8) -> Uuid {
        Uuid::from_bytes([seed; 16])
    }

    fn table(schema: Option<&str>, name: &str) -> TableEntry {
        TableEntry {
            schema: schema.map(str::to_string),
            name: name.to_string(),
        }
    }

    #[test]
    fn source_side_builds_connection_database_schema_table_hierarchy_with_payloads() {
        let profile_id = uuid(1);
        let roots = vec![ConnRoot {
            profile_id,
            label: "Prod".to_string(),
            databases: vec![DbNode {
                name: "app".to_string(),
                schemas: vec![SchemaNode {
                    name: "public".to_string(),
                    tables: vec![
                        table(Some("public"), "users"),
                        table(Some("public"), "orders"),
                    ],
                }],
                tables: Vec::new(),
            }],
        }];

        let mut model = TreeModel::new();
        let nodes = build_side_nodes(TreeSide::Source, &roots, &mut model);

        assert_eq!(nodes.len(), 1);
        let connection = &nodes[0];
        assert_eq!(connection.id, connection_node_id(profile_id));
        assert!(!connection.selectable);

        let database = &connection.children[0];
        assert_eq!(database.id, database_node_id(profile_id, "app"));
        assert!(!database.selectable);

        let schema = &database.children[0];
        assert_eq!(schema.id, schema_node_id(profile_id, "app", "public"));

        let user_leaf = &schema.children[0];
        assert_eq!(
            user_leaf.id,
            table_node_id(profile_id, "app", Some("public"), "users")
        );
        assert!(user_leaf.selectable);

        assert_eq!(
            model.payload(&connection_node_id(profile_id)),
            Some(&TreePayload::Connection(profile_id))
        );
        assert!(matches!(
            model.payload(&table_node_id(profile_id, "app", Some("public"), "users")),
            Some(TreePayload::Table { .. })
        ));
    }

    #[test]
    fn unloaded_source_database_gets_a_status_child_so_it_stays_expandable() {
        let profile_id = uuid(2);
        let roots = vec![ConnRoot {
            profile_id,
            label: "Prod".to_string(),
            databases: vec![DbNode {
                name: "other".to_string(),
                ..Default::default()
            }],
        }];

        let mut model = TreeModel::new();
        let nodes = build_side_nodes(TreeSide::Source, &roots, &mut model);

        let database = &nodes[0].children[0];
        assert_eq!(database.children.len(), 1);
        assert!(is_status_id(&database.children[0].id));
        assert!(!database.children[0].selectable);
    }

    #[test]
    fn failed_source_database_gets_a_selectable_retry_child() {
        let profile_id = uuid(3);
        let roots = vec![ConnRoot {
            profile_id,
            label: "Prod".to_string(),
            databases: vec![DbNode {
                name: "other".to_string(),
                ..Default::default()
            }],
        }];

        let mut model = TreeModel::new();
        model.set_load(
            database_node_id(profile_id, "other"),
            NodeLoad::Failed("boom".to_string()),
        );
        let nodes = build_side_nodes(TreeSide::Source, &roots, &mut model);

        let retry = &nodes[0].children[0].children[0];
        assert!(is_retry_id(&retry.id));
        assert!(retry.selectable);
        assert!(retry.label.contains("boom"));
    }

    #[test]
    fn target_side_stops_at_selectable_database_leaves() {
        let profile_id = uuid(4);
        let roots = vec![ConnRoot {
            profile_id,
            label: "Warehouse".to_string(),
            databases: vec![DbNode {
                name: "analytics".to_string(),
                ..Default::default()
            }],
        }];

        let mut model = TreeModel::new();
        let nodes = build_side_nodes(TreeSide::Target, &roots, &mut model);

        let database = &nodes[0].children[0];
        assert_eq!(database.id, database_node_id(profile_id, "analytics"));
        assert!(database.selectable);
        assert!(database.children.is_empty());
        assert!(matches!(
            model.payload(&database_node_id(profile_id, "analytics")),
            Some(TreePayload::Database { .. })
        ));
    }

    #[test]
    fn split_tables_by_schema_separates_schemaless_from_grouped() {
        let tables = vec![
            table(None, "loose"),
            table(Some("public"), "users"),
            table(Some("public"), "orders"),
            table(Some("audit"), "log"),
        ];

        let (schemaless, schemas) = split_tables_by_schema(tables);

        assert_eq!(schemaless.len(), 1);
        assert_eq!(schemaless[0].name, "loose");

        assert_eq!(schemas.len(), 2);
        let public = schemas.iter().find(|s| s.name == "public").unwrap();
        assert_eq!(public.tables.len(), 2);
        let audit = schemas.iter().find(|s| s.name == "audit").unwrap();
        assert_eq!(audit.tables.len(), 1);
    }

    fn table_payload(
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        name: &str,
    ) -> TreePayload {
        TreePayload::Table {
            profile_id,
            database: database.to_string(),
            schema: schema.map(str::to_string),
            table: TableRef {
                schema: schema.map(str::to_string),
                name: name.to_string(),
            },
        }
    }

    #[test]
    fn checked_source_tables_never_cross_the_resolved_source_database() {
        let profile_id = uuid(9);
        let mut model = TreeModel::new();

        // Same table name in two different databases — the exact silent
        // cross-database mismatch W1 guards against.
        let active_users = table_node_id(profile_id, "app", Some("public"), "users");
        let other_users = table_node_id(profile_id, "archive", Some("public"), "users");
        let other_orders = table_node_id(profile_id, "archive", Some("public"), "orders");

        model.insert_payload(
            active_users.clone(),
            table_payload(profile_id, "app", Some("public"), "users"),
        );
        model.insert_payload(
            other_users.clone(),
            table_payload(profile_id, "archive", Some("public"), "users"),
        );
        model.insert_payload(
            other_orders.clone(),
            table_payload(profile_id, "archive", Some("public"), "orders"),
        );

        model.toggle_checked(&active_users);
        model.toggle_checked(&other_users);
        model.toggle_checked(&other_orders);
        assert_eq!(model.checked_count(), 3);

        let resolved = checked_tables_in_database(&model, "app");

        // Only the table in the active source database survives — the
        // same-named "users" and the "orders" in "archive" are dropped, so a
        // cross-database source table can never reach the plan.
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].name, "users");

        assert!(is_source_table_checkable(
            model.payload(&active_users),
            "app"
        ));
        assert!(!is_source_table_checkable(
            model.payload(&other_users),
            "app"
        ));
        assert!(!is_source_table_checkable(
            model.payload(&other_orders),
            "app"
        ));
    }

    #[test]
    fn parent_of_synthetic_recovers_the_real_node_id() {
        assert_eq!(parent_of_synthetic("db:1:app::__status"), Some("db:1:app"));
        assert_eq!(parent_of_synthetic("conn:1::__retry"), Some("conn:1"));
        assert_eq!(parent_of_synthetic("db:1:app"), None);
    }
}
