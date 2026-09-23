//! Shared, driver-agnostic DB object hierarchy for the sidebar and the
//! migration wizard.
//!
//! This module owns two things:
//!
//! 1. **Hierarchy construction** ([`project_object_tree`],
//!    [`project_profile_tree`]): a generic snapshot of the
//!    profile → database → schema → table structure derived from the existing
//!    `AppState` caches (the connected profile's schema snapshot, the cached
//!    database list, per-database connection slots, and the lazy per-database
//!    schema cache). It never opens a second authoritative cache and never
//!    branches on a driver id — everything is driven by the schema-loading
//!    strategy, the `dbflux_core::DataStructure` paradigm, and cache
//!    presence. Collections, metrics, buckets, keyspaces, and other
//!    driver-specific node families stay consumer-local by design.
//!
//! 2. **One asynchronous coordinator** ([`ObjectTreeCoordinator`], owned by
//!    [`crate::AppStateEntity`]) that deduplicates, cancels, and settles the
//!    driver work behind those nodes through the session-fenced core seams.
//!    Consumers keep independent expansion, cursor, selection, checks,
//!    filters, and presentation; they only share this construction and this
//!    coordinator. Consumer adapters must not double-report failures: the
//!    coordinator reports user-facing errors once through the centralized
//!    `report_error` seam and exposes [`ObjectTreeOutcome::Failed`] for
//!    per-node state.
//!
//! Typed keys ([`ObjectTreeKey`]) are the identity contract: the same object
//! name under a different profile, database, or schema always yields a
//! different key, and rebuilds are key-stable. Keys are never parsed out of
//! strings.

mod coordinator;

#[cfg(test)]
pub(crate) mod test_support;

pub use coordinator::{
    ObjectTreeCoordinator, ObjectTreeEvent, ObjectTreeInstallKey, ObjectTreeOutcome,
    ObjectTreeRejection, ObjectTreeRequestKey, ObjectTreeRequestStatus,
};

use std::collections::BTreeMap;

use dbflux_core::{RelationalSchema, SchemaSnapshotAuthority, TableInfo};
use gpui::SharedString;
use uuid::Uuid;

use crate::app_state_entity::AppStateEntity;

/// Display label for the implicit, empty-named database of a single-database
/// driver (e.g. SQLite, whose one database is conventionally called `main`),
/// so a tree row is never blank. Keyed off an empty database name, not a
/// driver id, so it stays driver-agnostic. Consumers may override labels
/// locally; the key keeps the real (possibly empty) database identity.
pub const IMPLICIT_DATABASE_LABEL: &str = "main";

/// The label shown for a database node, falling back to
/// [`IMPLICIT_DATABASE_LABEL`] for the empty-named implicit database.
pub fn database_display_label(name: &str) -> SharedString {
    if name.trim().is_empty() {
        SharedString::from(IMPLICIT_DATABASE_LABEL)
    } else {
        SharedString::from(name.to_string())
    }
}

/// Typed, immutable identity of one node in the shared object hierarchy.
///
/// Every field is owned data: two nodes with the same display name in
/// different profiles, databases, or schemas never collide, and rebuilding
/// the hierarchy from the same caches reproduces equal keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ObjectTreeKey {
    Profile {
        profile_id: Uuid,
    },
    Database {
        profile_id: Uuid,
        database: String,
    },
    Schema {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    Table {
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        table: String,
    },
    View {
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        view: String,
    },
}

impl ObjectTreeKey {
    /// The profile every node under this key belongs to.
    pub fn profile_id(&self) -> Uuid {
        match self {
            ObjectTreeKey::Profile { profile_id } => *profile_id,
            ObjectTreeKey::Database { profile_id, .. }
            | ObjectTreeKey::Schema { profile_id, .. }
            | ObjectTreeKey::Table { profile_id, .. }
            | ObjectTreeKey::View { profile_id, .. } => *profile_id,
        }
    }

    /// The key of the parent node, `None` for profile roots.
    pub fn parent(&self) -> Option<ObjectTreeKey> {
        match self {
            ObjectTreeKey::Profile { .. } => None,
            ObjectTreeKey::Database {
                profile_id,
                database: _,
            } => Some(ObjectTreeKey::Profile {
                profile_id: *profile_id,
            }),
            ObjectTreeKey::Schema {
                profile_id,
                database,
                schema: _,
            } => Some(ObjectTreeKey::Database {
                profile_id: *profile_id,
                database: database.clone(),
            }),
            ObjectTreeKey::Table {
                profile_id,
                database,
                schema,
                table: _,
            }
            | ObjectTreeKey::View {
                profile_id,
                database,
                schema,
                view: _,
            } => Some(match schema {
                Some(schema) => ObjectTreeKey::Schema {
                    profile_id: *profile_id,
                    database: database.clone(),
                    schema: schema.clone(),
                },
                // A schema-less table hangs directly under its database.
                None => ObjectTreeKey::Database {
                    profile_id: *profile_id,
                    database: database.clone(),
                },
            }),
        }
    }

    /// The database segment of the key, when the node lives under one.
    pub fn database(&self) -> Option<&str> {
        match self {
            ObjectTreeKey::Profile { .. } => None,
            ObjectTreeKey::Database { database, .. } => Some(database),
            ObjectTreeKey::Schema { database, .. } => Some(database),
            ObjectTreeKey::Table { database, .. } | ObjectTreeKey::View { database, .. } => {
                Some(database)
            }
        }
    }
}

/// Whether a hierarchy node's content is known from the existing caches.
///
/// `Empty` is a *cached success with no children* (for example a database
/// that genuinely has no tables); it must stay distinguishable from
/// `Unloaded` so consumers never show a misleading "nothing here" for data
/// that was simply never fetched. Loading/failed overlays come from the
/// coordinator, not from this snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeContent {
    /// No cached data describes this node's children yet.
    #[default]
    Unloaded,
    /// Cached data describes the children listed on the node.
    Loaded,
    /// Cached data was consulted and the node genuinely has no children.
    Empty,
}

/// One node of the shared hierarchy snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTreeNode {
    pub key: ObjectTreeKey,
    pub label: String,
    pub state: NodeContent,
    pub children: Vec<ObjectTreeNode>,
}

impl Default for ObjectTreeNode {
    fn default() -> Self {
        Self {
            key: ObjectTreeKey::Profile {
                profile_id: Uuid::nil(),
            },
            label: String::new(),
            state: NodeContent::default(),
            children: Vec::new(),
        }
    }
}

impl ObjectTreeNode {
    /// Depth-first lookup by key.
    pub fn find(&self, key: &ObjectTreeKey) -> Option<&ObjectTreeNode> {
        if &self.key == key {
            return Some(self);
        }
        self.children.iter().find_map(|child| child.find(key))
    }
}

/// A generic hierarchy snapshot projected from the existing app caches.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectTreeSnapshot {
    /// One root per connected profile, in connection-map iteration order.
    pub roots: Vec<ObjectTreeNode>,
}

impl ObjectTreeSnapshot {
    /// Depth-first lookup by key across all profile roots.
    pub fn find(&self, key: &ObjectTreeKey) -> Option<&ObjectTreeNode> {
        self.roots.iter().find_map(|root| root.find(key))
    }

    /// The root node of one connected profile.
    pub fn profile(&self, profile_id: Uuid) -> Option<&ObjectTreeNode> {
        self.roots
            .iter()
            .find(|root| root.key.profile_id() == profile_id)
    }
}

/// Projects the shared hierarchy for every connected profile from the
/// existing `AppState` caches. Pure over the app state: no driver calls, no
/// second cache, no writes.
pub fn project_object_tree(state: &AppStateEntity) -> ObjectTreeSnapshot {
    let mut roots = Vec::new();
    for (profile_id, connected) in state.connections() {
        roots.push(profile_node(
            *profile_id,
            &profile_view(state, *profile_id, connected),
        ));
    }
    ObjectTreeSnapshot { roots }
}

/// Projects the shared hierarchy of one connected profile.
pub fn project_profile_tree(state: &AppStateEntity, profile_id: Uuid) -> Option<ObjectTreeNode> {
    let connected = state.connections().get(&profile_id)?;
    Some(profile_node(
        profile_id,
        &profile_view(state, profile_id, connected),
    ))
}

/// Projects the shared hierarchy node for one explicit, already-known
/// database of a connected profile, through the exact same
/// [`database_node`]/[`profile_view`] path the root projection uses.
///
/// Root enumeration deliberately names databases only from the cached
/// database list, the schema snapshot, and the implicit fallback — it must
/// never resurrect an authoritative empty list. Consumers that know a
/// database identity independently of that enumeration (the migration
/// wizard's resolved source database) use this helper to project that one
/// node, including its per-database schema cache content, without a second
/// hierarchy implementation.
pub fn project_database_node(
    state: &AppStateEntity,
    profile_id: Uuid,
    database: &str,
) -> Option<ObjectTreeNode> {
    Some(project_database_with_metadata(state, profile_id, database)?.node)
}

/// A database hierarchy and the exact cache source selected to construct it.
/// Consumers can resolve details without repeating authority/slot precedence.
pub struct ProjectedDatabase<'a> {
    pub node: ObjectTreeNode,
    source: DatabaseSource<'a>,
}

impl ProjectedDatabase<'_> {
    /// Metadata for a projected table, scoped by its complete typed key.
    pub fn table(&self, key: &ObjectTreeKey) -> Option<&TableInfo> {
        self.node.find(key)?;
        let ObjectTreeKey::Table { schema, table, .. } = key else {
            return None;
        };
        let matches =
            |candidate: &&TableInfo| candidate.name == *table && candidate.schema == *schema;
        match self.source {
            DatabaseSource::Relational(relational) => relational
                .schemas
                .iter()
                .flat_map(|group| group.tables.iter())
                .chain(relational.tables.iter())
                .find(matches),
            DatabaseSource::Lazy(info) => info.tables.iter().find(matches),
            DatabaseSource::Unloaded => None,
        }
    }

    /// Borrow types only from the authority-selected source and projected schema.
    /// `None` means not loaded; `Some(empty)` is an authoritative empty result.
    pub fn schema_types(&self, name: &str) -> Option<Vec<&dbflux_core::CustomTypeInfo>> {
        let ObjectTreeKey::Database { database, .. } = &self.node.key else {
            return None;
        };
        let has_schema = self.node.children.iter().any(
            |child| matches!(&child.key, ObjectTreeKey::Schema { schema, .. } if schema == name),
        );
        let empty_database_fallback = self.node.children.is_empty() && database == name;
        if !(has_schema || empty_database_fallback) {
            return None;
        }
        match self.source {
            DatabaseSource::Relational(relational) => {
                let mut loaded = false;
                let mut types = Vec::new();
                for group in &relational.schemas {
                    if let Some(group_types) = &group.custom_types {
                        loaded = true;
                        types.extend(
                            group_types.iter().filter(|kind| {
                                kind.schema.as_deref().unwrap_or(&group.name) == name
                            }),
                        );
                    }
                }
                loaded.then_some(types)
            }
            DatabaseSource::Lazy(info) => info.custom_types.as_ref().map(|types| {
                types
                    .iter()
                    .filter(|kind| {
                        kind.schema.as_deref().unwrap_or(if info.name.is_empty() {
                            database
                        } else {
                            &info.name
                        }) == name
                    })
                    .collect()
            }),
            DatabaseSource::Unloaded => None,
        }
    }
}

pub fn project_database_with_metadata<'a>(
    state: &'a AppStateEntity,
    profile_id: Uuid,
    database: &str,
) -> Option<ProjectedDatabase<'a>> {
    let connected = state.connections().get(&profile_id)?;
    Some(database_projection(
        profile_id,
        database,
        &profile_view(state, profile_id, connected),
    ))
}

fn profile_view<'a>(
    state: &'a AppStateEntity,
    profile_id: Uuid,
    connected: &'a dbflux_core::ConnectedProfile,
) -> ConnectedProfileView<'a> {
    ConnectedProfileView {
        connected,
        listed: state.get_database_list(profile_id).map(Vec::as_slice),
        primary_authority: state.schema_snapshot_authority(profile_id),
    }
}

// --- Projection internals ---

/// The immutable slice of one connected profile the projection reads.
struct ConnectedProfileView<'a> {
    connected: &'a dbflux_core::ConnectedProfile,
    listed: Option<&'a [dbflux_core::DatabaseInfo]>,
    primary_authority: Option<SchemaSnapshotAuthority>,
}

fn profile_node(profile_id: Uuid, view: &ConnectedProfileView<'_>) -> ObjectTreeNode {
    let children: Vec<ObjectTreeNode> = database_names(view)
        .iter()
        .map(|name| database_node(profile_id, name, view))
        .collect();
    // A profile with a snapshot or a cached list has its database set
    // resolved; anything else has not been enumerated yet.
    let state = if view.listed.is_some() || view.connected.schema.is_some() {
        if children.is_empty() {
            NodeContent::Empty
        } else {
            NodeContent::Loaded
        }
    } else {
        NodeContent::Unloaded
    };
    ObjectTreeNode {
        key: ObjectTreeKey::Profile { profile_id },
        label: view.connected.profile.name.clone(),
        state,
        children,
    }
}

/// Enumerates the database identities known for one connected profile,
/// preserving encounter order. A cached list owns enumeration; an empty
/// success permits only an independently authoritative single-database
/// primary snapshot to supply an implicit identity.
fn database_names(view: &ConnectedProfileView<'_>) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    fn push(names: &mut Vec<String>, name: &str) {
        if !names.iter().any(|existing| existing == name) {
            names.push(name.to_string());
        }
    }

    // An empty cached list is a cached success — the snapshot's databases
    // must not resurrect a list the driver answered as empty.
    if let Some(listed) = view.listed {
        for info in listed {
            push(&mut names, &info.name);
        }
    } else {
        for info in view
            .connected
            .schema
            .iter()
            .flat_map(|schema| schema.databases())
        {
            push(&mut names, &info.name);
        }
    }

    let implicit_allowed = match view.listed {
        None => true,
        Some(listed) => {
            listed.is_empty()
                && view.connected.connection.schema_loading_strategy()
                    == dbflux_core::SchemaLoadingStrategy::SingleDatabase
                && view.primary_authority == Some(SchemaSnapshotAuthority::Authoritative)
                && view.connected.schema.as_ref().is_some_and(|snapshot| {
                    snapshot.databases().is_empty()
                        && (!snapshot.schemas().is_empty() || !snapshot.tables().is_empty())
                })
        }
    };
    if names.is_empty()
        && implicit_allowed
        && let Some(implicit) = implicit_database(view)
    {
        push(&mut names, &implicit);
    }
    names
}

/// The implicit database identity used when nothing lists databases: the
/// snapshot's current database, else the profile's active database, else an
/// empty name when the primary snapshot itself carries schemas or tables.
fn implicit_database(view: &ConnectedProfileView<'_>) -> Option<String> {
    let connected = view.connected;
    if let Some(current) = connected
        .schema
        .as_ref()
        .and_then(|schema| schema.current_database())
    {
        return Some(current.to_string());
    }
    if let Some(active) = connected.active_database.as_deref() {
        return Some(active.to_string());
    }
    // The primary snapshot itself carries schemas/tables under an unnamed
    // single database (e.g. SQLite): the implicit database keeps the empty
    // name and the shared label fallback makes it readable.
    let has_content = connected
        .schema
        .as_ref()
        .is_some_and(|schema| !schema.schemas().is_empty() || !schema.tables().is_empty());
    has_content.then(String::new)
}

enum DatabaseSource<'a> {
    Relational(&'a RelationalSchema),
    Lazy(&'a dbflux_core::DbSchemaInfo),
    Unloaded,
}

fn database_node(profile_id: Uuid, name: &str, view: &ConnectedProfileView<'_>) -> ObjectTreeNode {
    database_projection(profile_id, name, view).node
}

fn database_projection<'a>(
    profile_id: Uuid,
    name: &str,
    view: &ConnectedProfileView<'a>,
) -> ProjectedDatabase<'a> {
    let source = if let Some(relational) = relational_content_for_database(view, name) {
        DatabaseSource::Relational(relational)
    } else if let Some(info) = view.connected.database_schemas.get(name) {
        DatabaseSource::Lazy(info)
    } else {
        DatabaseSource::Unloaded
    };
    let (state, children) = match source {
        DatabaseSource::Relational(relational) => {
            content_state(children_from_relational(profile_id, name, relational))
        }
        DatabaseSource::Lazy(info) => {
            content_state(children_from_db_schema(profile_id, name, info))
        }
        DatabaseSource::Unloaded => (NodeContent::Unloaded, Vec::new()),
    };
    ProjectedDatabase {
        node: ObjectTreeNode {
            key: ObjectTreeKey::Database {
                profile_id,
                database: name.to_string(),
            },
            label: database_display_label(name).to_string(),
            state,
            children,
        },
        source,
    }
}

fn content_state(children: Vec<ObjectTreeNode>) -> (NodeContent, Vec<ObjectTreeNode>) {
    if children.is_empty() {
        (NodeContent::Empty, children)
    } else {
        (NodeContent::Loaded, children)
    }
}

/// The relational content describing one database, if any cached source
/// describes it: the primary snapshot for the current database, or the
/// database's own connection slot.
fn relational_content_for_database<'a>(
    view: &ConnectedProfileView<'a>,
    database: &str,
) -> Option<&'a RelationalSchema> {
    let connected = view.connected;
    if view.primary_authority != Some(SchemaSnapshotAuthority::EnumerationOnly)
        && let Some(snapshot) = connected.schema.as_ref()
    {
        let is_current = snapshot.current_database() == Some(database)
            // The unnamed implicit single database owns the primary
            // snapshot's schema-less content.
            || (database.is_empty() && snapshot.current_database().is_none());
        if is_current && let Some(relational) = snapshot.as_relational() {
            return Some(relational);
        }
    }
    // The target database's own connection slot: its snapshot describes
    // exactly that database, populated or authoritatively empty. It is the
    // same cache the wizard's source picker consumes; the primary snapshot
    // of a DIFFERENT database must never stand in for it.
    connected
        .database_connection(database)
        .and_then(|slot| slot.schema.as_ref())
        .and_then(|schema| schema.as_relational())
}

/// Builds schema/table children from a relational schema: snapshot schema
/// groups keep their encounter order, and tables without a schema are
/// children of the database itself (schema-less sources).
fn children_from_relational(
    profile_id: Uuid,
    database: &str,
    relational: &RelationalSchema,
) -> Vec<ObjectTreeNode> {
    let mut children = Vec::new();
    for schema in &relational.schemas {
        let mut schema_children: Vec<ObjectTreeNode> = schema
            .tables
            .iter()
            .map(|table| table_node(profile_id, database, table))
            .collect();
        schema_children.extend(
            schema
                .views
                .iter()
                .map(|view| view_node(profile_id, database, view)),
        );
        children.push(ObjectTreeNode {
            key: ObjectTreeKey::Schema {
                profile_id,
                database: database.to_string(),
                schema: schema.name.clone(),
            },
            label: schema.name.clone(),
            state: if schema_children.is_empty()
                && schema.custom_types.as_ref().is_none_or(Vec::is_empty)
            {
                NodeContent::Empty
            } else {
                NodeContent::Loaded
            },
            children: schema_children,
        });
    }
    // Top-level tables group by their own schema field (matching the lazy
    // fetch path), sorted so rebuilds are deterministic.
    let (groups, schemaless) = split_tables_by_schema(profile_id, database, &relational.tables);
    for (name, group_children) in groups {
        children.push(ObjectTreeNode {
            key: ObjectTreeKey::Schema {
                profile_id,
                database: database.to_string(),
                schema: name.clone(),
            },
            label: name,
            state: NodeContent::Loaded,
            children: group_children,
        });
    }
    children.extend(schemaless);
    append_views(profile_id, database, &relational.views, &mut children);
    for group in &relational.schemas {
        if let Some(types) = &group.custom_types {
            append_type_schemas(profile_id, database, &group.name, types, &mut children);
        }
    }
    children
}

/// Builds schema/table children from a lazily cached
/// [`dbflux_core::DbSchemaInfo`], grouping by each table's own schema field
/// exactly like the wizard does.
fn children_from_db_schema(
    profile_id: Uuid,
    database: &str,
    info: &dbflux_core::DbSchemaInfo,
) -> Vec<ObjectTreeNode> {
    let (groups, schemaless) = split_tables_by_schema(profile_id, database, &info.tables);
    let mut children = Vec::new();
    for (name, group_children) in groups {
        children.push(ObjectTreeNode {
            key: ObjectTreeKey::Schema {
                profile_id,
                database: database.to_string(),
                schema: name.clone(),
            },
            label: name,
            state: NodeContent::Loaded,
            children: group_children,
        });
    }
    children.extend(schemaless);
    append_views(profile_id, database, &info.views, &mut children);
    if let Some(types) = &info.custom_types {
        let fallback = if info.name.is_empty() {
            database
        } else {
            &info.name
        };
        append_type_schemas(profile_id, database, fallback, types, &mut children);
    }
    children.sort_by(|left, right| match (&left.key, &right.key) {
        (
            ObjectTreeKey::Schema { schema: left, .. },
            ObjectTreeKey::Schema { schema: right, .. },
        ) => left.cmp(right),
        (ObjectTreeKey::Schema { .. }, _) => std::cmp::Ordering::Less,
        (_, ObjectTreeKey::Schema { .. }) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    });
    children
}

fn append_type_schemas(
    profile_id: Uuid,
    database: &str,
    fallback: &str,
    types: &[dbflux_core::CustomTypeInfo],
    children: &mut Vec<ObjectTreeNode>,
) {
    for kind in types {
        let schema = kind.schema.as_deref().unwrap_or(fallback);
        if let Some(node) = children.iter_mut().find(|child| {
            matches!(&child.key, ObjectTreeKey::Schema { schema: name, .. } if name == schema)
        }) {
            node.state = NodeContent::Loaded;
        } else {
            children.push(ObjectTreeNode {
                key: ObjectTreeKey::Schema {
                    profile_id,
                    database: database.to_string(),
                    schema: schema.to_string(),
                },
                label: schema.to_string(),
                state: NodeContent::Loaded,
                children: Vec::new(),
            });
        }
    }
}

fn append_views(
    profile_id: Uuid,
    database: &str,
    views: &[dbflux_core::ViewInfo],
    children: &mut Vec<ObjectTreeNode>,
) {
    for view in views {
        let node = view_node(profile_id, database, view);
        if let Some(schema) = &view.schema {
            if let Some(group) = children.iter_mut().find(|child| {
                matches!(&child.key, ObjectTreeKey::Schema { schema: name, .. } if name == schema)
            }) {
                group.children.push(node);
                group.state = NodeContent::Loaded;
                continue;
            }
            children.push(ObjectTreeNode {
                key: ObjectTreeKey::Schema {
                    profile_id,
                    database: database.to_string(),
                    schema: schema.clone(),
                },
                label: schema.clone(),
                state: NodeContent::Loaded,
                children: vec![node],
            });
        } else {
            children.push(node);
        }
    }
}

fn view_node(profile_id: Uuid, database: &str, view: &dbflux_core::ViewInfo) -> ObjectTreeNode {
    ObjectTreeNode {
        key: ObjectTreeKey::View {
            profile_id,
            database: database.to_string(),
            schema: view.schema.clone(),
            view: view.name.clone(),
        },
        label: view.name.clone(),
        state: NodeContent::Loaded,
        children: Vec::new(),
    }
}

/// Groups tables by their own schema field: `None` stays schema-less under
/// the database, `Some` groups alphabetically (deterministic across
/// rebuilds). Returns `(schema_groups, schemaless_tables)`. Pure.
fn split_tables_by_schema(
    profile_id: Uuid,
    database: &str,
    tables: &[TableInfo],
) -> (BTreeMap<String, Vec<ObjectTreeNode>>, Vec<ObjectTreeNode>) {
    let mut groups: BTreeMap<String, Vec<ObjectTreeNode>> = BTreeMap::new();
    let mut schemaless = Vec::new();
    for table in tables {
        match &table.schema {
            Some(schema) => groups
                .entry(schema.clone())
                .or_default()
                .push(table_node(profile_id, database, table)),
            None => schemaless.push(table_node(profile_id, database, table)),
        }
    }
    (groups, schemaless)
}

/// Builds the leaf node for one table.
fn table_node(profile_id: Uuid, database: &str, table: &TableInfo) -> ObjectTreeNode {
    ObjectTreeNode {
        key: ObjectTreeKey::Table {
            profile_id,
            database: database.to_string(),
            schema: table.schema.clone(),
            table: table.name.clone(),
        },
        label: table.name.clone(),
        state: NodeContent::Loaded,
        children: Vec::new(),
    }
}
