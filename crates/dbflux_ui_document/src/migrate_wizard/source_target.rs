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
//! Both pickers consume the shared `dbflux_ui_base::object_tree` layer: the
//! hierarchy is re-projected from the app caches via
//! [`project_object_tree`], and lazy loads (database lists, per-database
//! schemas, guarded per-database slot installs) run through the
//! [`ObjectTreeRequestKey`] coordinator owned by the same `AppStateEntity`
//! the sidebar adapter uses. The phase subscribes to [`ObjectTreeEvent`] for
//! its lifetime; closing the wizard drops the phase and its subscription
//! without cancelling another consumer's shared requests. The wizard keeps
//! no per-node load state: pending comes from the coordinator and the last
//! settled outcome drives the retry rows.
//!
//! `TreeNav` is reused as the pure nav model (rows / expand / cursor). The
//! checkbox state lives in the wizard-owned [`TreeModel`] (design ADR #3/#4)
//! — `TreeNav` renders nothing itself, so the checkbox glyph is drawn here
//! over `TreeModel::is_checked`, and an unloaded/loading/failed branch
//! surfaces a synthetic child row (`Loading…` / `Retry`) so the branch stays
//! expandable and its state is visible. The wizard entity mounts this phase
//! as the first step of the flow.

use std::collections::HashSet;

use dbflux_components::components::tree_nav::{
    TreeNav, TreeNavAction, TreeNavNode, render_gutter, tree_line_color,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Spacing};
use dbflux_core::{TableRef, transfer_compatible};
use dbflux_ui_base::app_state_entity::AppStateEntity;
use dbflux_ui_base::object_tree::{
    NodeContent, ObjectTreeEvent, ObjectTreeKey, ObjectTreeNode, ObjectTreeOutcome,
    ObjectTreeRejection, ObjectTreeRequestKey, ObjectTreeSnapshot, database_display_label,
    project_database_node, project_object_tree,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

use crate::migrate_wizard::phases::can_advance_from_source_target;
use crate::migrate_wizard::tree_model::{TreeModel, TreePayload, object_tree_node_id};

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

/// Why a branch has no renderable children yet, derived from the shared
/// coordinator's pending flag and last settled outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BranchStatus {
    /// The request is in flight (or was never attempted): show `Loading…`.
    Loading,
    /// The last attempt failed, was rejected, or was cancelled: show an
    /// explicit `Retry` row. Neither a rebuild nor an expand re-dispatches
    /// these — retrying is the user's explicit choice.
    Retry(SharedString),
}

/// Short status descriptor for a structured coordinator rejection, rendered
/// inside the localized `Retry — %{error}` row. Derived from the typed
/// rejection only — the coordinator's failure messages are never parsed here.
fn rejection_descriptor(reason: ObjectTreeRejection) -> &'static str {
    match reason {
        ObjectTreeRejection::ProfileDisconnected => "profile disconnected",
        ObjectTreeRejection::ConnectionReplaced => "connection replaced",
        ObjectTreeRejection::RequestInvalidated => "request invalidated",
        ObjectTreeRejection::TargetSlotReplaced => "database connection replaced",
    }
}

/// Status descriptor for a cancelled shared request.
const CANCELLED_DESCRIPTOR: &str = "cancelled";

/// Derives a branch's status from the shared coordinator: pending wins, then
/// the settled outcome, then the "not yet fetched" placeholder.
fn branch_status(state: &AppStateEntity, key: &ObjectTreeRequestKey) -> BranchStatus {
    if state.object_tree_is_pending(key) {
        return BranchStatus::Loading;
    }
    match state.object_tree_outcome(key) {
        Some(ObjectTreeOutcome::Failed(error)) => {
            BranchStatus::Retry(SharedString::from(error.clone()))
        }
        Some(ObjectTreeOutcome::Rejected(reason)) => {
            BranchStatus::Retry(rejection_descriptor(*reason).into())
        }
        Some(ObjectTreeOutcome::Cancelled) => BranchStatus::Retry(CANCELLED_DESCRIPTOR.into()),
        _ => BranchStatus::Loading,
    }
}

/// The database-list request behind a profile-level node.
fn list_request_key(key: &ObjectTreeKey) -> ObjectTreeRequestKey {
    ObjectTreeRequestKey::DatabaseList {
        profile_id: key.profile_id(),
    }
}

/// The schema request behind a database-level node.
fn database_request_key(key: &ObjectTreeKey) -> ObjectTreeRequestKey {
    ObjectTreeRequestKey::DatabaseSchema {
        profile_id: key.profile_id(),
        database: key.database().unwrap_or_default().to_string(),
    }
}

/// The synthetic child rendered under an expandable branch whose contents are
/// not yet available, so the branch stays expandable (a childless `TreeNav`
/// group is not) and its load state is visible.
fn synthetic_status_child(parent: &SharedString, status: BranchStatus) -> TreeNavNode {
    match status {
        BranchStatus::Loading => {
            let mut node = TreeNavNode::leaf(
                status_child_id(parent),
                dbflux_i18n::t!("document.migrate_wizard.footer.loading"),
                Some(AppIcon::Loader),
            );
            node.selectable = false;
            node
        }
        BranchStatus::Retry(error) => {
            let mut node = TreeNavNode::leaf(
                retry_child_id(parent),
                dbflux_i18n::t!("document.migrate_wizard.source_target.retry", error = error),
                Some(AppIcon::RotateCcw),
            );
            node.selectable = true;
            node
        }
    }
}

/// The wizard-owned payload for one shared-hierarchy node, so click and key
/// handlers act on ids without parsing them.
fn tree_payload(key: &ObjectTreeKey) -> TreePayload {
    match key {
        ObjectTreeKey::Profile { profile_id } => TreePayload::Connection(*profile_id),
        ObjectTreeKey::Database {
            profile_id,
            database,
        } => TreePayload::Database {
            profile_id: *profile_id,
            database: database.clone(),
        },
        ObjectTreeKey::Schema {
            profile_id,
            database,
            schema,
        } => TreePayload::Schema {
            profile_id: *profile_id,
            database: database.clone(),
            schema: schema.clone(),
        },
        ObjectTreeKey::View { .. } => unreachable!("views are excluded from migration navigation"),
        ObjectTreeKey::Table {
            profile_id,
            database,
            schema,
            table,
        } => TreePayload::Table {
            profile_id: *profile_id,
            database: database.clone(),
            schema: schema.clone(),
            table: TableRef {
                schema: schema.clone(),
                name: table.clone(),
            },
        },
    }
}

/// Builds the full `TreeNav` node list for one side from the shared
/// projection's profile roots and the coordinator's status, registering every
/// real node's payload so click and key handlers can act on ids without
/// parsing them. `roots` are already side-scoped (source: the single source
/// profile with source-database-first ordering; target: the
/// transfer-compatible profiles sorted by label). Pure over its inputs.
pub(crate) fn build_side_nodes(
    side: TreeSide,
    roots: &[ObjectTreeNode],
    status_for: &dyn Fn(&ObjectTreeRequestKey) -> BranchStatus,
    model: &mut TreeModel,
) -> Vec<TreeNavNode> {
    roots
        .iter()
        .map(|root| object_node_to_nav(root, side, status_for, model))
        .collect()
}

/// Maps one shared-hierarchy node to its `TreeNav` row. Target databases are
/// selectable leaves (the container the migration loads into — per-table
/// mapping is the grid's job); source databases are groups carrying their
/// schemas/tables plus a synthetic status child while their shared request
/// has no renderable content yet.
fn object_node_to_nav(
    node: &ObjectTreeNode,
    side: TreeSide,
    status_for: &dyn Fn(&ObjectTreeRequestKey) -> BranchStatus,
    model: &mut TreeModel,
) -> TreeNavNode {
    let id = object_tree_node_id(&node.key);
    model.insert_payload(id.clone(), tree_payload(&node.key));

    match &node.key {
        ObjectTreeKey::View { .. } => unreachable!("views are excluded from migration navigation"),
        ObjectTreeKey::Table { .. } => {
            TreeNavNode::leaf(id, node.label.clone(), Some(AppIcon::Table))
        }
        ObjectTreeKey::Schema { .. } => {
            let children = node
                .children
                .iter()
                .filter(|child| !matches!(child.key, ObjectTreeKey::View { .. }))
                .map(|child| object_node_to_nav(child, side, status_for, model))
                .collect();
            TreeNavNode::group(id, node.label.clone(), Some(AppIcon::Folder), children)
        }
        ObjectTreeKey::Database { .. } => {
            let label = node.label.clone();
            match side {
                TreeSide::Target => TreeNavNode::leaf(id, label, Some(AppIcon::Database)),
                TreeSide::Source => {
                    let mut children: Vec<TreeNavNode> = node
                        .children
                        .iter()
                        .filter(|child| !matches!(child.key, ObjectTreeKey::View { .. }))
                        .map(|child| object_node_to_nav(child, side, status_for, model))
                        .collect();
                    // An authoritatively empty database stays a childless
                    // group; anything else without children is still waiting
                    // on its shared schema load.
                    if children.is_empty() && node.state != NodeContent::Empty {
                        children.push(synthetic_status_child(
                            &id,
                            status_for(&database_request_key(&node.key)),
                        ));
                    }
                    TreeNavNode::group(id, label, Some(AppIcon::Database), children)
                }
            }
        }
        ObjectTreeKey::Profile { .. } => {
            let mut children: Vec<TreeNavNode> = node
                .children
                .iter()
                .map(|child| object_node_to_nav(child, side, status_for, model))
                .collect();
            // A settled-empty profile (authoritative empty list/snapshot)
            // has no loading work behind it — the wizard-local implicit
            // target adaptation (see `target_roots`) supplies its
            // container instead. Anything else without children is still
            // waiting on its shared list load.
            if children.is_empty() && node.state != NodeContent::Empty {
                children.push(synthetic_status_child(
                    &id,
                    status_for(&list_request_key(&node.key)),
                ));
            }
            TreeNavNode::group(id, node.label.clone(), Some(AppIcon::Server), children)
        }
    }
}

/// The source profile's shared-hierarchy node with source-database-first
/// ordering applied: the database the migration reads from is always the
/// first child, and it is ensured even when the root enumeration does not
/// name it (no cached list/snapshot, or an authoritative empty list). The
/// ensured node comes from [`project_database_node`] — the same shared
/// builder the root projection uses — so the resolved database carries its
/// real cached content instead of a forever-unloaded synthetic row.
fn source_profile_node(
    snapshot: &ObjectTreeSnapshot,
    resolved_node: Option<ObjectTreeNode>,
    source_profile_id: Uuid,
    source_database: &str,
) -> Option<ObjectTreeNode> {
    let mut profile = snapshot.profile(source_profile_id)?.clone();

    let mut databases: Vec<ObjectTreeNode> = Vec::with_capacity(profile.children.len() + 1);
    if !source_database.is_empty() {
        match profile
            .children
            .iter()
            .position(|child| child.key.database() == Some(source_database))
        {
            Some(index) => databases.push(profile.children.remove(index)),
            None => {
                if let Some(node) = resolved_node {
                    databases.push(node);
                }
            }
        }
    }
    databases.extend(profile.children);
    profile.children = databases;
    Some(profile)
}

/// The target tree's shared-hierarchy roots: every connected profile that is
/// still transfer-compatible with the live source connection (the source
/// itself included — a same-container migration is legal), sorted by profile
/// label. Compatibility is re-read from the live connections so a
/// disconnected profile drops out of the target tree.
fn target_roots(
    snapshot: &ObjectTreeSnapshot,
    source_profile_id: Uuid,
    state: &AppStateEntity,
) -> Vec<ObjectTreeNode> {
    let Some(source_connected) = state.connections().get(&source_profile_id) else {
        return Vec::new();
    };
    let source_metadata = source_connected.connection.metadata();

    let mut roots: Vec<ObjectTreeNode> = snapshot
        .roots
        .iter()
        .filter(|root| {
            state
                .connections()
                .get(&root.key.profile_id())
                .is_some_and(|connected| {
                    transfer_compatible(source_metadata, connected.connection.metadata())
                })
        })
        .cloned()
        .collect();
    roots.sort_by(|a, b| a.label.cmp(&b.label));

    // Wizard-local implicit/default target adaptation, through the shared
    // explicit per-database projection: an authoritatively empty target
    // profile (a successful empty list, or a snapshot that names no
    // databases) still exposes one selectable implicit container — the
    // connection's active database when it names one, else the empty
    // single-database identity. This restores the pre-adapter behavior
    // without mutating global enumeration and without resurrecting a removed
    // named target: the identity comes from the connection, never from the
    // selection. Unloaded profiles get no fallback — their database set is
    // not known yet, and their shared list load is still outstanding.
    for root in &mut roots {
        if root.children.is_empty() && root.state == NodeContent::Empty {
            let profile_id = root.key.profile_id();
            let implicit_identity = state
                .connections()
                .get(&profile_id)
                .and_then(|connected| connected.connection.active_database())
                .unwrap_or_default();
            if let Some(node) = project_database_node(state, profile_id, &implicit_identity) {
                root.children.push(node);
            }
        }
    }

    roots
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
/// single source the plan is built for. Sorted by qualified name — the
/// checked set is a `HashSet`, and grid rows, Confirm rows, and the
/// FK-independent run order must be deterministic across openings.
fn checked_tables_in_database(model: &TreeModel, source_database: &str) -> Vec<TableRef> {
    let mut tables: Vec<TableRef> = model
        .checked_ids()
        .filter_map(|id| match model.payload(id) {
            Some(TreePayload::Table {
                database, table, ..
            }) if database == source_database => Some(table.clone()),
            _ => None,
        })
        .collect();

    tables.sort_by_key(|table| table.qualified_name());
    tables
}

/// Whether the shared coordinator still owes this branch a first load: not
/// pending, never settled with a failure/rejection/cancellation (an explicit
/// `Retry` row owns those), and the shared projection does not already
/// describe the node's content (a cached success — loaded or authoritatively
/// empty — needs no driver work). A schema request consults the explicit
/// per-database projection because its node may live outside the root
/// enumeration; every other request consults the root projection.
fn shared_request_wanted(state: &AppStateEntity, key: &ObjectTreeRequestKey) -> bool {
    if state.object_tree_is_pending(key) {
        return false;
    }
    if matches!(
        state.object_tree_outcome(key),
        Some(
            ObjectTreeOutcome::Failed(_)
                | ObjectTreeOutcome::Rejected(_)
                | ObjectTreeOutcome::Cancelled
        )
    ) {
        return false;
    }
    let node = match key {
        ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database,
        } => project_database_node(state, *profile_id, database),
        _ => project_object_tree(state).find(&key.node_key()).cloned(),
    };
    !matches!(
        node.as_ref().map(|node| &node.state),
        Some(NodeContent::Loaded | NodeContent::Empty)
    )
}

/// Per-side runtime state: the payload/checked model and the `TreeNav` nav
/// state built from the shared projection. The hierarchy itself is not kept
/// locally — every rebuild re-projects it from the app caches, and loading
/// state is read from the shared coordinator.
struct SideState {
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
    /// The phase's subscription to the shared coordinator's settle events,
    /// held for the phase's lifetime. Dropping the phase (closing/reopening
    /// the wizard) unsubscribes; it must never cancel the coordinator's
    /// requests, which other consumers may share.
    _object_tree_sub: Subscription,
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
        let resolved_database =
            Self::resolve_source_database(&app_state, source_profile_id, source_database, cx);

        let mut source_model = TreeModel::new();
        let seed = source_model.seed_source_selection(
            source_profile_id,
            &resolved_database,
            &source_tables,
        );
        let mut target_model = TreeModel::new();

        let (source_tree, target_tree) = {
            let state = app_state.read(cx);
            let snapshot = project_object_tree(state);
            let status_for = |key: &ObjectTreeRequestKey| branch_status(state, key);

            let resolved_node = if resolved_database.is_empty() {
                None
            } else {
                project_database_node(state, source_profile_id, &resolved_database)
            };
            let source_roots = source_profile_node(
                &snapshot,
                resolved_node,
                source_profile_id,
                &resolved_database,
            )
            .into_iter()
            .collect::<Vec<_>>();
            let source_nodes = build_side_nodes(
                TreeSide::Source,
                &source_roots,
                &status_for,
                &mut source_model,
            );
            let mut source_tree = TreeNav::new(source_nodes, seed.expand);
            if let Some(cursor) = seed.cursor {
                source_tree.select_by_id(&cursor);
            }

            let target_roots = target_roots(&snapshot, source_profile_id, state);
            let target_nodes = build_side_nodes(
                TreeSide::Target,
                &target_roots,
                &status_for,
                &mut target_model,
            );
            let target_tree = TreeNav::new(target_nodes, HashSet::new());
            (source_tree, target_tree)
        };

        let _object_tree_sub =
            cx.subscribe(&app_state, |this, _state, event: &ObjectTreeEvent, cx| {
                this.on_object_tree_event(event, cx);
            });

        let mut phase = Self {
            app_state,
            focus_handle: cx.focus_handle(),
            source_profile_id,
            source_database: resolved_database,
            source: SideState {
                model: source_model,
                tree: source_tree,
            },
            target: SideState {
                model: target_model,
                tree: target_tree,
            },
            active_side: TreeSide::Source,
            target_selection: None,
            error: None,
            _object_tree_sub,
        };

        phase.ensure_resolved_database_loaded(cx);
        phase
    }

    /// The database this migration reads from: the sidebar's explicit
    /// selection, else the connected snapshot's current database, else the
    /// connection's active database, else empty (the implicit single-database
    /// identity is kept in the tree keys, not the phase).
    fn resolve_source_database(
        app_state: &Entity<AppStateEntity>,
        source_profile_id: Uuid,
        source_database: Option<String>,
        cx: &App,
    ) -> String {
        let connected = app_state.read(cx).connections().get(&source_profile_id);
        source_database
            .or_else(|| {
                connected.and_then(|connected| {
                    connected
                        .schema
                        .as_ref()
                        .and_then(|schema| schema.current_database())
                        .map(str::to_string)
                })
            })
            .or_else(|| connected.and_then(|connected| connected.connection.active_database()))
            .unwrap_or_default()
    }

    /// Kicks the shared-coordinator loads for the resolved source database
    /// when neither the shared projection nor a settled attempt describes
    /// them: the node arrives pre-expanded with pre-checked tables, so
    /// without an initial request the seeded checks would stay unresolvable
    /// ghosts and the placeholder row would read "Loading…" with nothing in
    /// flight. The profile's database list is requested first — the shared
    /// projection enumerates a lazy profile's databases from that cached
    /// list — then the database's schema. Exactly one kick each: an already
    /// failed/rejected/cancelled request waits for the user's explicit
    /// `Retry`.
    fn ensure_resolved_database_loaded(&mut self, cx: &mut Context<Self>) {
        if self.source_database.is_empty() {
            return;
        }

        let list_key = ObjectTreeRequestKey::DatabaseList {
            profile_id: self.source_profile_id,
        };
        let schema_key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id: self.source_profile_id,
            database: self.source_database.clone(),
        };
        let mut requested = false;
        if shared_request_wanted(self.app_state.read(cx), &list_key) {
            self.app_state.update(cx, |state, cx| {
                state.object_tree_request(list_key.clone(), cx);
            });
            requested = true;
        }
        if shared_request_wanted(self.app_state.read(cx), &schema_key) {
            self.app_state.update(cx, |state, cx| {
                state.object_tree_request(schema_key, cx);
            });
            requested = true;
        }
        if requested {
            self.rebuild_side(TreeSide::Source, cx);
            cx.notify();
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

    /// Whether the phase's advance guard is satisfied, via the tested pure
    /// guard [`can_advance_from_source_target`]: at least one checked source
    /// table that actually resolves to a real table payload in the source
    /// database (a raw checked count would let ghost checks enable Continue
    /// for a "migrate 0 tables" run), a chosen target container, and live
    /// transfer compatibility between the two connections.
    pub fn is_ready(&self, cx: &App) -> bool {
        can_advance_from_source_target(
            self.checked_source_tables().len(),
            self.target_selection.is_some(),
            self.target_is_transfer_compatible(cx),
        )
    }

    /// Re-verifies transfer compatibility against the live connections. The
    /// target tree only lists compatible profiles, but either side can
    /// disconnect while the phase is open.
    fn target_is_transfer_compatible(&self, cx: &App) -> bool {
        let Some(selection) = self.target_selection.as_ref() else {
            return false;
        };

        let state = self.app_state.read(cx);
        let connections = state.connections();
        match (
            connections.get(&self.source_profile_id),
            connections.get(&selection.profile_id),
        ) {
            (Some(source), Some(target)) => {
                transfer_compatible(source.connection.metadata(), target.connection.metadata())
            }
            _ => false,
        }
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

    /// Re-projects the shared hierarchy and rebuilds one side's `TreeNav`
    /// rows. Expansion, cursor, checked state, and payloads are preserved by
    /// `TreeNav::set_nodes` and the model — only the rendered rows change.
    fn rebuild_side(&mut self, side: TreeSide, cx: &mut Context<Self>) {
        let target_invalidated = {
            let state = self.app_state.read(cx);
            let snapshot = project_object_tree(state);
            let status_for = |key: &ObjectTreeRequestKey| branch_status(state, key);

            let roots = match side {
                TreeSide::Source => {
                    let resolved_node = if self.source_database.is_empty() {
                        None
                    } else {
                        project_database_node(state, self.source_profile_id, &self.source_database)
                    };
                    source_profile_node(
                        &snapshot,
                        resolved_node,
                        self.source_profile_id,
                        &self.source_database,
                    )
                    .into_iter()
                    .collect::<Vec<_>>()
                }
                TreeSide::Target => target_roots(&snapshot, self.source_profile_id, state),
            };

            let side_state = self.side_mut(side);
            // Replace (not merge) payload membership with the current projection.
            side_state.model.clear_payloads();
            let nodes = build_side_nodes(side, &roots, &status_for, &mut side_state.model);
            side_state.tree.set_nodes(nodes);

            // Reconcile the effective target selection against the CURRENT
            // valid containers.
            match side {
                TreeSide::Target => self
                    .target_selection
                    .as_ref()
                    .is_some_and(|selection| !self.target_selection_is_current(&roots, selection)),
                TreeSide::Source => false,
            }
        };
        if target_invalidated {
            self.target_selection = None;
            // The host must re-evaluate: downstream mapping/advance state
            // built on the removed container can no longer stand.
            cx.emit(SourceTargetChanged);
        }
    }

    /// Whether `selection` is still one of the target containers the current
    /// rebuild actually rendered. Validation runs against the SAME adapted
    /// root set the tree was built from — connected, transfer-compatible
    /// profiles carrying their current database sets, including the implicit
    /// container `target_roots` derives from the CURRENT connection for
    /// authoritatively empty profiles. A valid implicit-fallback selection
    /// therefore survives shared settles, while a disconnected or
    /// incompatible profile, an enumerated named-target removal, or a changed
    /// fallback identity still invalidates it. An unloaded database set has
    /// not proven the container gone, so the selection is retained.
    fn target_selection_is_current(
        &self,
        adapted_roots: &[ObjectTreeNode],
        selection: &TargetSelection,
    ) -> bool {
        let Some(root) = adapted_roots
            .iter()
            .find(|root| root.key.profile_id() == selection.profile_id)
        else {
            // Not rendered this rebuild: the profile is disconnected or no
            // longer transfer-compatible with the source.
            return false;
        };
        if root.state == NodeContent::Unloaded {
            return true;
        }
        root.children
            .iter()
            .any(|child| child.key.database() == Some(selection.database.as_str()))
    }

    /// A shared-coordinator settle arrived: re-project both pickers (a
    /// profile's list/schema settle can reshape either tree) and, when the
    /// resolved source database settled successfully, ask the host to
    /// re-evaluate its advance guard — freshly settled payloads can turn
    /// seeded checks into resolvable tables (or reveal them as ghosts).
    fn on_object_tree_event(&mut self, event: &ObjectTreeEvent, cx: &mut Context<Self>) {
        self.rebuild_side(TreeSide::Source, cx);
        self.rebuild_side(TreeSide::Target, cx);

        if let ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database,
        } = &event.key
            && *profile_id == self.source_profile_id
            && *database == self.source_database
            && matches!(
                event.outcome,
                ObjectTreeOutcome::Applied | ObjectTreeOutcome::Cached
            )
        {
            cx.emit(SourceTargetChanged);
        }
        cx.notify();
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
                    self.error = Some(dbflux_i18n::t!(
                        "document.migrate_wizard.source_target.cross_database_error",
                        source = self.source_database,
                        other = database
                    ));
                    cx.notify();
                }
            }
            Some(TreePayload::Database {
                profile_id,
                database,
            }) if side == TreeSide::Target => {
                let selection = TargetSelection {
                    profile_id,
                    database,
                };

                // Re-selecting the already-chosen target is a no-op: emitting
                // would make the host discard downstream mapping work.
                if self.target_selection.as_ref() != Some(&selection) {
                    self.target_selection = Some(selection);
                    cx.emit(SourceTargetChanged);
                }
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
        let request_key = if expanded {
            self.side(side)
                .model
                .payload(&id)
                .and_then(TreePayload::request_key)
        } else {
            None
        };
        if let Some(key) = request_key
            && shared_request_wanted(self.app_state.read(cx), &key)
        {
            self.app_state.update(cx, |state, cx| {
                state.object_tree_request(key.clone(), cx);
            });
            self.rebuild_side(side, cx);
        }
        cx.notify();
    }

    /// Explicit retry from the `Retry` row: re-dispatches the branch's shared
    /// request through the coordinator (cancelling nothing but the settled
    /// attempt's leftovers) and rebuilds so the row flips back to `Loading…`.
    fn retry(&mut self, side: TreeSide, retry_id: &str, cx: &mut Context<Self>) {
        let Some(parent) = parent_of_synthetic(retry_id) else {
            return;
        };
        let Some(key) = self
            .side(side)
            .model
            .payload(parent)
            .and_then(TreePayload::request_key)
        else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            state.object_tree_retry(key, cx);
        });
        self.rebuild_side(side, cx);
        cx.notify();
    }

    fn handle_key_down(
        &mut self,
        event: &KeyDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let modifiers = event.keystroke.modifiers;

        // Shift+Tab switches the active tree; plain Tab is deliberately left
        // unhandled so it can move focus onward to the footer's Continue button
        // (keyboard-first — the wizard must never trap Tab on this phase).
        if modifiers.shift && event.keystroke.key == "tab" {
            self.active_side = match self.active_side {
                TreeSide::Source => TreeSide::Target,
                TreeSide::Target => TreeSide::Source,
            };
            cx.notify();
            return;
        }

        if modifiers != Modifiers::none() {
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
                    .child(self.render_tree_panel(TreeSide::Source, cx))
                    .child(self.render_tree_panel(TreeSide::Target, cx)),
            )
            .when_some(self.error.clone(), |parent, error| {
                parent.child(Text::caption(error).danger())
            })
    }
}

impl SourceTargetPhase {
    fn render_tree_panel(&self, side: TreeSide, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();

        // `id_segment` stays a fixed, untranslated ASCII literal — the GPUI
        // element id must never be built from catalog-resolved text.
        let (title, id_segment) = match side {
            TreeSide::Source => (
                dbflux_i18n::t!("document.migrate_wizard.source_target.source_title"),
                "source",
            ),
            TreeSide::Target => (
                dbflux_i18n::t!("document.migrate_wizard.source_target.target_title"),
                "target",
            ),
        };
        let subtitle = match side {
            TreeSide::Source => crate::labels::migrate_source_target_checked_count_label(
                self.source.model.checked_count(),
            ),
            TreeSide::Target => self
                .target_selection
                .as_ref()
                .map(|selection| selection.database.clone())
                .unwrap_or_else(|| {
                    dbflux_i18n::t!("document.migrate_wizard.source_target.no_target_selected")
                }),
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
                    .child(Text::body(title))
                    .child(Text::caption(subtitle)),
            )
            .child(
                div()
                    .id(SharedString::from(format!("migrate-tree-{id_segment}")))
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

#[cfg(test)]
mod tests {
    use super::{
        BranchStatus, TreeSide, build_side_nodes, checked_tables_in_database, is_retry_id,
        is_status_id, parent_of_synthetic, rejection_descriptor, source_profile_node,
    };
    use crate::migrate_wizard::tree_model::{
        TreeModel, TreePayload, connection_node_id, database_node_id, schema_node_id, table_node_id,
    };
    use dbflux_core::TableRef;
    use dbflux_ui_base::object_tree::{
        NodeContent, ObjectTreeKey, ObjectTreeNode, ObjectTreeRejection, ObjectTreeRequestKey,
        ObjectTreeSnapshot, database_display_label,
    };
    use uuid::Uuid;

    fn uuid(seed: u8) -> Uuid {
        Uuid::from_bytes([seed; 16])
    }

    fn profile_root(
        profile_id: Uuid,
        label: &str,
        state: NodeContent,
        databases: Vec<ObjectTreeNode>,
    ) -> ObjectTreeNode {
        ObjectTreeNode {
            key: ObjectTreeKey::Profile { profile_id },
            label: label.to_string(),
            state,
            children: databases,
        }
    }

    fn database_root(
        profile_id: Uuid,
        name: &str,
        state: NodeContent,
        children: Vec<ObjectTreeNode>,
    ) -> ObjectTreeNode {
        ObjectTreeNode {
            key: ObjectTreeKey::Database {
                profile_id,
                database: name.to_string(),
            },
            label: database_display_label(name).to_string(),
            state,
            children,
        }
    }

    fn schema_node(
        profile_id: Uuid,
        database: &str,
        name: &str,
        tables: Vec<ObjectTreeNode>,
    ) -> ObjectTreeNode {
        ObjectTreeNode {
            key: ObjectTreeKey::Schema {
                profile_id,
                database: database.to_string(),
                schema: name.to_string(),
            },
            label: name.to_string(),
            state: if tables.is_empty() {
                NodeContent::Empty
            } else {
                NodeContent::Loaded
            },
            children: tables,
        }
    }

    fn table_leaf(
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        name: &str,
    ) -> ObjectTreeNode {
        ObjectTreeNode {
            key: ObjectTreeKey::Table {
                profile_id,
                database: database.to_string(),
                schema: schema.map(str::to_string),
                table: name.to_string(),
            },
            label: name.to_string(),
            state: NodeContent::Loaded,
            children: Vec::new(),
        }
    }

    /// Status stub: every branch reports `Loading` unless the test overrides
    /// the schema requests with a `Retry`.
    fn loading_status(_key: &ObjectTreeRequestKey) -> BranchStatus {
        BranchStatus::Loading
    }

    fn retry_for_schema(error: &'static str) -> impl Fn(&ObjectTreeRequestKey) -> BranchStatus {
        move |key| match key {
            ObjectTreeRequestKey::DatabaseSchema { .. } => BranchStatus::Retry(error.into()),
            _ => BranchStatus::Loading,
        }
    }

    #[test]
    fn source_side_excludes_views_from_navigation_and_table_payloads() {
        let profile_id = uuid(1);
        let view = ObjectTreeNode {
            key: ObjectTreeKey::View {
                profile_id,
                database: "app".into(),
                schema: Some("public".into()),
                view: "report".into(),
            },
            label: "report".into(),
            state: NodeContent::Loaded,
            children: Vec::new(),
        };
        let table = table_leaf(profile_id, "app", Some("public"), "users");
        let roots = vec![profile_root(
            profile_id,
            "Prod",
            NodeContent::Loaded,
            vec![database_root(
                profile_id,
                "app",
                NodeContent::Loaded,
                vec![schema_node(profile_id, "app", "public", vec![table, view])],
            )],
        )];
        let mut model = TreeModel::new();
        let nodes = build_side_nodes(TreeSide::Source, &roots, &loading_status, &mut model);
        let schema = &nodes[0].children[0].children[0];
        assert_eq!(schema.children.len(), 1);
        assert_eq!(
            schema.children[0].id,
            table_node_id(profile_id, "app", Some("public"), "users")
        );
        assert!(model.payload(&schema.children[0].id).is_some());
    }

    #[test]
    fn source_side_builds_connection_database_schema_table_hierarchy_with_payloads() {
        let profile_id = uuid(1);
        let roots = vec![profile_root(
            profile_id,
            "Prod",
            NodeContent::Loaded,
            vec![database_root(
                profile_id,
                "app",
                NodeContent::Loaded,
                vec![schema_node(
                    profile_id,
                    "app",
                    "public",
                    vec![
                        table_leaf(profile_id, "app", Some("public"), "users"),
                        table_leaf(profile_id, "app", Some("public"), "orders"),
                    ],
                )],
            )],
        )];

        let mut model = TreeModel::new();
        let nodes = build_side_nodes(TreeSide::Source, &roots, &loading_status, &mut model);

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
        let roots = vec![profile_root(
            profile_id,
            "Prod",
            NodeContent::Loaded,
            vec![database_root(
                profile_id,
                "other",
                NodeContent::Unloaded,
                Vec::new(),
            )],
        )];

        let mut model = TreeModel::new();
        let nodes = build_side_nodes(TreeSide::Source, &roots, &loading_status, &mut model);

        let database = &nodes[0].children[0];
        assert_eq!(database.children.len(), 1);
        assert!(is_status_id(&database.children[0].id));
        assert!(!database.children[0].selectable);
    }

    #[test]
    fn authoritatively_empty_source_database_gets_no_status_child() {
        let profile_id = uuid(5);
        let roots = vec![profile_root(
            profile_id,
            "Prod",
            NodeContent::Loaded,
            vec![database_root(
                profile_id,
                "blank",
                NodeContent::Empty,
                Vec::new(),
            )],
        )];

        let mut model = TreeModel::new();
        let nodes = build_side_nodes(TreeSide::Source, &roots, &loading_status, &mut model);

        let database = &nodes[0].children[0];
        assert!(database.children.is_empty());
    }

    #[test]
    fn failed_source_database_gets_a_selectable_retry_child() {
        let profile_id = uuid(3);
        let roots = vec![profile_root(
            profile_id,
            "Prod",
            NodeContent::Loaded,
            vec![database_root(
                profile_id,
                "other",
                NodeContent::Unloaded,
                Vec::new(),
            )],
        )];

        let mut model = TreeModel::new();
        let nodes = build_side_nodes(
            TreeSide::Source,
            &roots,
            &retry_for_schema("boom"),
            &mut model,
        );

        let retry = &nodes[0].children[0].children[0];
        assert!(is_retry_id(&retry.id));
        assert!(retry.selectable);
        assert!(retry.label.contains("boom"));
    }

    #[test]
    fn source_database_is_ordered_first_and_ensured_when_missing_from_the_projection() {
        let profile_id = uuid(6);
        let snapshot = ObjectTreeSnapshot {
            roots: vec![profile_root(
                profile_id,
                "Prod",
                NodeContent::Loaded,
                vec![
                    database_root(profile_id, "analytics", NodeContent::Loaded, Vec::new()),
                    database_root(profile_id, "app", NodeContent::Loaded, Vec::new()),
                ],
            )],
        };

        // The resolved source database moves ahead of the projection order.
        let profile = source_profile_node(&snapshot, None, profile_id, "app").expect("profile");
        let names: Vec<&str> = profile
            .children
            .iter()
            .filter_map(|child| child.key.database())
            .collect();
        assert_eq!(names, vec!["app", "analytics"]);

        // A resolved database the projection does not name is ensured as the
        // first child from the explicit shared projection, so pre-seeded
        // checks have a branch that carries the database's real cached
        // content once it loads.
        let explicit_unloaded = ObjectTreeNode {
            key: ObjectTreeKey::Database {
                profile_id,
                database: "archive".to_string(),
            },
            label: database_display_label("archive").to_string(),
            state: NodeContent::Unloaded,
            children: Vec::new(),
        };
        let profile =
            source_profile_node(&snapshot, Some(explicit_unloaded), profile_id, "archive")
                .expect("profile");
        let first = &profile.children[0];
        assert_eq!(first.key.database(), Some("archive"));
        assert_eq!(first.state, NodeContent::Unloaded);
        assert_eq!(
            profile.children[1].key.database(),
            Some("analytics"),
            "the projection's own databases follow in their original order"
        );

        // An empty resolved database leaves the projection's children alone.
        let profile = source_profile_node(&snapshot, None, profile_id, "").expect("profile");
        assert_eq!(profile.children.len(), 2);
    }

    #[test]
    fn target_side_stops_at_selectable_database_leaves() {
        let profile_id = uuid(4);
        let roots = vec![profile_root(
            profile_id,
            "Warehouse",
            NodeContent::Loaded,
            vec![database_root(
                profile_id,
                "analytics",
                NodeContent::Loaded,
                vec![schema_node(
                    profile_id,
                    "analytics",
                    "public",
                    vec![table_leaf(profile_id, "analytics", Some("public"), "facts")],
                )],
            )],
        )];

        let mut model = TreeModel::new();
        let nodes = build_side_nodes(TreeSide::Target, &roots, &loading_status, &mut model);

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
    fn rejection_descriptors_are_structured_distinct_and_non_empty() {
        let descriptors: Vec<&str> = [
            ObjectTreeRejection::ProfileDisconnected,
            ObjectTreeRejection::ConnectionReplaced,
            ObjectTreeRejection::RequestInvalidated,
            ObjectTreeRejection::TargetSlotReplaced,
        ]
        .iter()
        .map(|reason| rejection_descriptor(*reason))
        .collect();

        for descriptor in &descriptors {
            assert!(!descriptor.is_empty());
        }
        for (index, descriptor) in descriptors.iter().enumerate() {
            for other in descriptors.iter().skip(index + 1) {
                assert_ne!(descriptor, other);
            }
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
            TreePayload::Table {
                profile_id,
                database: "app".to_string(),
                schema: Some("public".to_string()),
                table: TableRef {
                    schema: Some("public".to_string()),
                    name: "users".to_string(),
                },
            },
        );
        model.insert_payload(
            other_users.clone(),
            TreePayload::Table {
                profile_id,
                database: "archive".to_string(),
                schema: Some("public".to_string()),
                table: TableRef {
                    schema: Some("public".to_string()),
                    name: "users".to_string(),
                },
            },
        );
        model.insert_payload(
            other_orders.clone(),
            TreePayload::Table {
                profile_id,
                database: "archive".to_string(),
                schema: Some("public".to_string()),
                table: TableRef {
                    schema: Some("public".to_string()),
                    name: "orders".to_string(),
                },
            },
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

        assert!(is_source_table_checkable_helper(
            model.payload(&active_users),
            "app"
        ));
        assert!(!is_source_table_checkable_helper(
            model.payload(&other_users),
            "app"
        ));
        assert!(!is_source_table_checkable_helper(
            model.payload(&other_orders),
            "app"
        ));
    }

    fn is_source_table_checkable_helper(
        payload: Option<&TreePayload>,
        source_database: &str,
    ) -> bool {
        super::is_source_table_checkable(payload, source_database)
    }

    #[test]
    fn parent_of_synthetic_recovers_the_real_node_id() {
        assert_eq!(parent_of_synthetic("db:1:app::__status"), Some("db:1:app"));
        assert_eq!(parent_of_synthetic("conn:1::__retry"), Some("conn:1"));
        assert_eq!(parent_of_synthetic("db:1:app"), None);
    }

    // ── PR 27a-2: migrate_wizard/source_target.rs ──

    const MIGRATE_SOURCE_TARGET_KEYS: &[&str] = &[
        "document.migrate_wizard.source_target.source_title",
        "document.migrate_wizard.source_target.target_title",
        "document.migrate_wizard.source_target.checked_count.one",
        "document.migrate_wizard.source_target.checked_count.many",
        "document.migrate_wizard.source_target.no_target_selected",
        "document.migrate_wizard.source_target.retry",
        "document.migrate_wizard.source_target.source_connection_gone",
        "document.migrate_wizard.source_target.target_connection_gone",
        "document.migrate_wizard.source_target.cross_database_error",
        // Reused from PR 27a (`footer.loading`), listed here because the
        // unloaded-branch placeholder row resolves it from this file too.
        "document.migrate_wizard.footer.loading",
    ];

    /// Every `document.migrate_wizard.source_target.*` key introduced by this
    /// file (plus the key it reuses from PR 27a) resolves to a non-empty,
    /// non-fallback value in both locales.
    #[test]
    fn migrate_source_target_keys_resolve_in_both_locales() {
        for key in MIGRATE_SOURCE_TARGET_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, *key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    /// A representative sample of `document.migrate_wizard.source_target.*`
    /// keys diverges between locales.
    #[test]
    fn migrate_source_target_keys_differ_between_locales() {
        for key in [
            "document.migrate_wizard.source_target.source_title",
            "document.migrate_wizard.source_target.no_target_selected",
            "document.migrate_wizard.source_target.retry",
            "document.migrate_wizard.source_target.cross_database_error",
        ] {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert_ne!(en, es, "{key} must differ between en and es");
        }
    }
}

// ── Shared-coordinator integration (T3 wizard adapter) ──
//
// These tests run a real `SourceTargetPhase` against a real `AppStateEntity`
// (in-memory storage, controllable fake connections) and assert that the
// picker's lazy loads go through the shared object-tree coordinator — the
// same coordinator the sidebar adapter will use — instead of the wizard's
// former direct `schema_for_database`/`list_databases` background calls.
#[cfg(test)]
pub(crate) mod shared_coordinator_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use dbflux_components::components::tree_nav::TreeNavNode;
    use dbflux_core::{
        Connection, DatabaseCategory, DatabaseInfo, DbConfig, DbError, DbKind, DriverCapabilities,
        DriverFormDef, DriverMetadata, FormValues, Icon, QueryHandle, QueryRequest, QueryResult,
        RelationalSchema, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo,
        TransferFamily, WritePrivilege,
    };
    use gpui::{AppContext, Entity, IntoElement, Render, TestAppContext, div};
    use uuid::Uuid;

    use super::super::tree_model::{
        connection_node_id, database_node_id, object_tree_node_id, table_node_id,
    };
    use super::SourceTargetPhase;
    use super::TreeSide;
    use dbflux_core::TableRef;
    use dbflux_ui_base::app_state_entity::AppStateEntity;
    use dbflux_ui_base::object_tree::{
        ObjectTreeEvent, ObjectTreeOutcome, ObjectTreeRejection, ObjectTreeRequestKey,
    };

    pub(crate) const DRIVER_KEY: &str = "builtin:migrate-picker-test-driver";

    fn driver_metadata() -> DriverMetadata {
        DriverMetadata {
            id: "migrate-picker-test-conn".to_string(),
            display_name: "TestPicker".to_string(),
            description: "test".to_string(),
            category: DatabaseCategory::Relational,
            transfer_family: TransferFamily::Sql,
            deployment_class: None,
            query_language: dbflux_core::QueryLanguage::Sql,
            capabilities: DriverCapabilities::empty(),
            default_port: Some(5432),
            uri_scheme: "test".to_string(),
            icon: Icon::Database,
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
            editor_profile: None,
        }
    }

    pub(crate) fn test_table(schema: Option<&str>, name: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: schema.map(str::to_string),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
            storage_hints: None,
        }
    }

    pub(crate) fn db_schema(name: &str, tables: Vec<TableInfo>) -> dbflux_core::DbSchemaInfo {
        dbflux_core::DbSchemaInfo {
            name: name.to_string(),
            tables,
            views: Vec::new(),
            custom_types: None,
        }
    }

    fn relational_schema(
        databases: Vec<DatabaseInfo>,
        current: Option<&str>,
        schemas: Vec<dbflux_core::DbSchemaInfo>,
        tables: Vec<TableInfo>,
    ) -> SchemaSnapshot {
        SchemaSnapshot::relational(RelationalSchema {
            databases,
            current_database: current.map(str::to_string),
            schemas,
            tables,
            views: Vec::new(),
        })
    }

    /// A controllable fake connection: schema/database work is served from
    /// injected maps, call counters let tests assert who ran the driver work.
    pub(crate) struct PickerFakeConnection {
        pub(crate) metadata: DriverMetadata,
        strategy: SchemaLoadingStrategy,
        pub(crate) databases: Mutex<Vec<DatabaseInfo>>,
        pub(crate) schemas: Mutex<HashMap<String, dbflux_core::DbSchemaInfo>>,
        schema_failures: Mutex<HashMap<String, usize>>,
        schema_calls: Mutex<Vec<String>>,
        list_calls: AtomicUsize,
        list_failures: AtomicUsize,
        active: Mutex<Option<String>>,
    }

    impl PickerFakeConnection {
        pub(crate) fn new(strategy: SchemaLoadingStrategy) -> Arc<Self> {
            Arc::new(Self {
                metadata: driver_metadata(),
                strategy,
                databases: Mutex::new(Vec::new()),
                schemas: Mutex::new(HashMap::new()),
                schema_failures: Mutex::new(HashMap::new()),
                schema_calls: Mutex::new(Vec::new()),
                list_calls: AtomicUsize::new(0),
                list_failures: AtomicUsize::new(0),
                active: Mutex::new(None),
            })
        }

        pub(crate) fn set_schema(&self, database: &str, schema: dbflux_core::DbSchemaInfo) {
            self.schemas
                .lock()
                .expect("schemas")
                .insert(database.to_string(), schema);
        }

        fn fail_schema_once(&self, database: &str) {
            self.schema_failures
                .lock()
                .expect("schema failures")
                .insert(database.to_string(), 1);
        }

        pub(crate) fn schema_calls(&self) -> Vec<String> {
            self.schema_calls.lock().expect("schema calls").clone()
        }

        pub(crate) fn set_active_database(&self, database: &str) {
            *self.active.lock().expect("active") = Some(database.to_string());
        }

        pub(crate) fn fail_list_once(&self) {
            self.list_failures.fetch_add(1, Ordering::SeqCst);
        }

        fn list_calls(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }
    }

    impl Connection for PickerFakeConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("test connection".to_string()))
        }

        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }

        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            self.strategy
        }

        fn dialect(&self) -> &dyn SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }

        fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            if self.list_failures.load(Ordering::SeqCst) > 0 {
                self.list_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(DbError::ConnectionFailed(dbflux_core::FormattedError::new(
                    "database listing refused",
                )));
            }
            Ok(self.databases.lock().expect("databases").clone())
        }

        fn schema_for_database(
            &self,
            database: &str,
        ) -> Result<dbflux_core::DbSchemaInfo, DbError> {
            self.schema_calls
                .lock()
                .expect("schema calls")
                .push(database.to_string());

            let mut failures = self.schema_failures.lock().expect("schema failures");
            if let Some(remaining) = failures.get_mut(database)
                && *remaining > 0
            {
                *remaining -= 1;
                return Err(DbError::ConnectionFailed(dbflux_core::FormattedError::new(
                    format!("introspection for '{database}' failed"),
                )));
            }
            drop(failures);

            self.schemas
                .lock()
                .expect("schemas")
                .get(database)
                .cloned()
                .ok_or_else(|| {
                    DbError::ConnectionFailed(dbflux_core::FormattedError::new(format!(
                        "no fake schema for '{database}'"
                    )))
                })
        }

        fn table_details(
            &self,
            _database: &str,
            schema: Option<&str>,
            table: &str,
        ) -> Result<TableInfo, DbError> {
            let mut info = test_table(schema, table);
            info.columns = Some(Vec::new());
            Ok(info)
        }

        fn active_database(&self) -> Option<String> {
            self.active.lock().expect("active").clone()
        }
    }

    /// Builds a real `AppStateEntity` over in-memory storage.
    pub(crate) fn test_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
        cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    dbflux_storage::bootstrap::StorageRuntime::in_memory()
                        .expect("test storage runtime"),
                )
                .expect("test app state")
            })
        })
    }

    /// Connects `profile_id`'s profile through the same seam production uses.
    pub(crate) fn connect_profile(
        state: &Entity<AppStateEntity>,
        cx: &mut TestAppContext,
        profile_id: Uuid,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        let mut profile =
            dbflux_core::ConnectionProfile::new("test-profile", DbConfig::default_postgres());
        profile.id = profile_id;
        profile.set_driver_id(DRIVER_KEY);
        state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile,
                connection,
                schema,
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
    }

    /// One consumer's ObjectTreeEvent recorder.
    struct Recorder {
        events: Arc<Mutex<Vec<ObjectTreeEvent>>>,
        _marker: Entity<RecorderMarker>,
    }

    struct RecorderMarker {
        _subscription: gpui::Subscription,
    }

    impl Render for RecorderMarker {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            div()
        }
    }

    impl Recorder {
        fn new(state: &Entity<AppStateEntity>, cx: &mut TestAppContext) -> Self {
            let events: Arc<Mutex<Vec<ObjectTreeEvent>>> = Arc::default();
            let sink = events.clone();
            let observed = state.clone();
            let marker = cx.update(|cx| {
                cx.new(|cx| RecorderMarker {
                    _subscription: cx.subscribe(
                        &observed,
                        move |_, _, event: &ObjectTreeEvent, _| {
                            sink.lock().expect("recorder events").push(event.clone());
                        },
                    ),
                })
            });
            Self {
                events,
                _marker: marker,
            }
        }

        fn events(&self) -> Vec<ObjectTreeEvent> {
            self.events.lock().expect("recorder events").clone()
        }
    }

    fn schema_request_key(profile_id: Uuid, database: &str) -> ObjectTreeRequestKey {
        ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: database.to_string(),
        }
    }

    fn flat_row_ids(phase: &SourceTargetPhase) -> Vec<gpui::SharedString> {
        phase
            .source
            .tree
            .rows()
            .iter()
            .map(|row| row.id.clone())
            .collect()
    }

    fn target_flat_row_ids(phase: &SourceTargetPhase) -> Vec<gpui::SharedString> {
        phase
            .target
            .tree
            .rows()
            .iter()
            .map(|row| row.id.clone())
            .collect()
    }

    #[gpui::test]
    fn source_database_load_runs_through_the_shared_coordinator(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        // The shared projection enumerates a lazy profile's databases from
        // the cached list, so the fake serves one like a real lazy driver.
        fake.databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "app".into(),
                is_current: true,
            });
        fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, profile_id, fake.clone(), None);

        let recorder = Recorder::new(&state, cx);
        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        let key = schema_request_key(profile_id, "app");
        assert_eq!(fake.schema_calls(), vec!["app".to_string()]);
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied),
            "the picker's source load must settle through the shared coordinator"
        );
        assert!(
            recorder
                .events()
                .iter()
                .any(|event| event.key == key && event.outcome == ObjectTreeOutcome::Applied),
            "the picker must observe the shared settle event"
        );
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));

        // The settled snapshot must reach the picker's rendered tree: moving
        // the cursor to the fetched table expands its ancestors, which only
        // works when the event-driven rebuild registered the new nodes.
        let users_row = table_node_id(profile_id, "app", Some("public"), "users");
        phase.update(cx, |phase, _| phase.source.tree.select_by_id(&users_row));
        assert!(
            phase.read_with(cx, |phase, _| flat_row_ids(phase).contains(&users_row)),
            "the fetched table must appear in the source tree"
        );
    }

    #[gpui::test]
    fn dropped_wizard_phase_leaves_shared_coordinator_work_running(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        fake.databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "app".into(),
                is_current: true,
            });
        fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, profile_id, fake.clone(), None);

        let recorder = Recorder::new(&state, cx);
        cx.update(|cx| {
            let phase = cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            });
            // Closing the wizard drops the phase (and its subscription) while
            // the load is still in flight. The phase must NOT cancel the
            // shared request another consumer may be waiting on.
            drop(phase);
        });
        cx.run_until_parked();

        let key = schema_request_key(profile_id, "app");
        assert_eq!(fake.schema_calls(), vec!["app".to_string()]);
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied),
            "dropping the wizard must not cancel or discard the shared load"
        );
        assert!(
            recorder
                .events()
                .iter()
                .any(|event| event.key == key && event.outcome == ObjectTreeOutcome::Applied)
        );
    }

    #[gpui::test]
    fn two_wizard_phases_share_one_driver_call_for_the_same_database(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        fake.databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "app".into(),
                is_current: true,
            });
        fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, profile_id, fake.clone(), None);

        let first = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        let second = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        assert_eq!(
            fake.schema_calls(),
            vec!["app".to_string()],
            "two pickers over the same database must share one driver execution"
        );

        let users_row = table_node_id(profile_id, "app", Some("public"), "users");
        for (name, phase) in [("first", &first), ("second", &second)] {
            phase.update(cx, |phase, _| phase.source.tree.select_by_id(&users_row));
            assert!(
                phase.read_with(cx, |phase, _| flat_row_ids(phase).contains(&users_row)),
                "phase {name} must render the shared load's result"
            );
        }
    }

    #[gpui::test]
    fn reconnect_during_source_load_rejects_the_stale_result_instead_of_applying_it(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "stale")]),
        );
        connect_profile(&state, cx, profile_id, fake.clone(), None);

        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });

        // Reconnect before the fetch lands: the session replacement must
        // fence the in-flight result instead of applying it into the picker.
        let replacement = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        replacement.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "fresh")]),
        );
        let mut profile =
            dbflux_core::ConnectionProfile::new("test-profile", DbConfig::default_postgres());
        profile.id = profile_id;
        profile.set_driver_id(DRIVER_KEY);
        state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile,
                replacement.clone() as Arc<dyn Connection>,
                None,
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
        cx.run_until_parked();

        let key = schema_request_key(profile_id, "app");
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Rejected(
                ObjectTreeRejection::ConnectionReplaced
            )),
        );
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));

        // The stale table must never reach the tree, and the node must not
        // stick in Loading.
        let stale_row = table_node_id(profile_id, "app", Some("public"), "stale");
        assert!(
            !phase.read_with(cx, |phase, _| flat_row_ids(phase).contains(&stale_row)),
            "a fenced stale result must not render in the picker"
        );
    }
    #[gpui::test]
    fn failed_source_load_requires_explicit_retry_and_recovers(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        for name in ["app", "other"] {
            fake.databases
                .lock()
                .expect("databases")
                .push(DatabaseInfo {
                    name: name.into(),
                    is_current: name == "app",
                });
        }
        fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        fake.set_schema("other", db_schema("other", vec![test_table(None, "loose")]));
        fake.fail_schema_once("other");
        connect_profile(&state, cx, profile_id, fake.clone(), None);

        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();
        assert_eq!(fake.schema_calls(), vec!["app".to_string()]);

        // Expanding the other database dispatches its schema load through the
        // shared coordinator; the first attempt fails.
        let other_id = dbflux_ui_base::object_tree::ObjectTreeKey::Database {
            profile_id,
            database: "other".to_string(),
        };
        let other_node_id = object_tree_node_id(&other_id);
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&other_node_id);
            phase.activate_current(TreeSide::Source, cx);
        });
        cx.run_until_parked();

        assert_eq!(
            fake.schema_calls(),
            vec!["app".to_string(), "other".to_string()],
        );
        let key = schema_request_key(profile_id, "other");
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Failed(_)),
        ));
        let retry_row = format!("{other_node_id}::__retry");
        assert!(
            phase.read_with(cx, |phase, _| flat_row_ids(phase)
                .iter()
                .any(|id| id.as_ref() == retry_row)),
            "the failed branch must surface its explicit Retry row"
        );

        // Revisiting the branch (and re-rendering) must NOT auto-retry the
        // failed request — retrying is the user's explicit choice.
        phase.update(cx, |phase, cx| phase.rebuild_side(TreeSide::Source, cx));
        assert_eq!(
            fake.schema_calls(),
            vec!["app".to_string(), "other".to_string()],
            "no automatic re-dispatch after a failed attempt"
        );

        // The user selects the Retry row: the shared request is retried and
        // the fetched table reaches the tree.
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&retry_row);
            phase.activate_current(TreeSide::Source, cx);
        });
        cx.run_until_parked();
        assert_eq!(fake.schema_calls().len(), 3, "the explicit retry re-ran");
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
            Some(ObjectTreeOutcome::Applied),
        ));

        let loose_row = table_node_id(profile_id, "other", None, "loose");
        phase.update(cx, |phase, _| phase.source.tree.select_by_id(&loose_row));
        assert!(
            phase.read_with(cx, |phase, _| flat_row_ids(phase).contains(&loose_row)),
            "the retried load's table must appear in the source tree"
        );
    }

    #[gpui::test]
    fn target_database_list_loads_through_the_shared_coordinator(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let source_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();
        let source_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        source_fake
            .databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "app".into(),
                is_current: true,
            });
        source_fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, source_id, source_fake.clone(), None);

        let target_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        target_fake
            .databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "warehouse".into(),
                is_current: true,
            });
        connect_profile(&state, cx, target_id, target_fake.clone(), None);

        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    source_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        // The target connection is listed but its database list is unloaded
        // until the user expands it — then it loads through the coordinator.
        assert_eq!(target_fake.list_calls(), 0);
        let connection_id = connection_node_id(target_id);
        phase.update(cx, |phase, cx| {
            phase.on_toggle(TreeSide::Target, connection_id, true, cx)
        });
        cx.run_until_parked();

        assert_eq!(target_fake.list_calls(), 1);
        let list_key = ObjectTreeRequestKey::DatabaseList {
            profile_id: target_id,
        };
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(&list_key).cloned()),
            Some(ObjectTreeOutcome::Applied),
        );

        let warehouse_leaf = database_node_id(target_id, "warehouse");
        phase.update(cx, |phase, _| {
            phase.target.tree.select_by_id(&warehouse_leaf)
        });
        phase.update(cx, |phase, cx| {
            phase.on_select(TreeSide::Target, warehouse_leaf.clone(), cx)
        });
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some("warehouse".to_string()),
        );
    }
    #[gpui::test]
    fn preseeded_checks_become_resolvable_after_the_shared_load_and_survive_rebuilds(
        cx: &mut TestAppContext,
    ) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        for name in ["app", "other"] {
            fake.databases
                .lock()
                .expect("databases")
                .push(DatabaseInfo {
                    name: name.into(),
                    is_current: name == "app",
                });
        }
        fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        fake.set_schema("other", db_schema("other", vec![test_table(None, "loose")]));
        connect_profile(&state, cx, profile_id, fake.clone(), None);

        // The sidebar opened the wizard pre-checking "users" in "app".
        let seeded = vec![TableRef {
            schema: Some("public".to_string()),
            name: "users".to_string(),
        }];
        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    seeded.clone(),
                    cx,
                )
            })
        });

        // Before the shared load settles, the checked id has no payload to
        // resolve against — the advance guard must see zero real tables.
        assert!(phase.read_with(cx, |phase, _| phase.checked_source_tables().is_empty()));

        cx.run_until_parked();
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.checked_source_tables()),
            seeded,
            "the settled shared snapshot must resolve the pre-seeded check"
        );

        // A later event-driven rebuild (another database's schema load) must
        // keep the checked state and its resolvable payload intact.
        let other_id = dbflux_ui_base::object_tree::ObjectTreeKey::Database {
            profile_id,
            database: "other".to_string(),
        };
        let other_node_id = object_tree_node_id(&other_id);
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&other_node_id);
            phase.activate_current(TreeSide::Source, cx);
        });
        cx.run_until_parked();

        assert_eq!(
            fake.schema_calls(),
            vec!["app".to_string(), "other".to_string()],
            "both schema loads ran through the shared coordinator"
        );
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.checked_source_tables()),
            seeded,
            "the pre-seeded check must survive shared snapshot rebuilds"
        );
        let loose_row = table_node_id(profile_id, "other", None, "loose");
        phase.update(cx, |phase, _| phase.source.tree.select_by_id(&loose_row));
        assert!(
            phase.read_with(cx, |phase, _| flat_row_ids(phase).contains(&loose_row)),
            "the rebuilt tree must render the other database's tables"
        );
    }
    /// A lazy source profile whose database is known by the wizard but named
    /// by neither a snapshot nor a cached database list: the schema request
    /// applies through the shared coordinator, so the resolved source
    /// database must show its tables instead of a forever-`Loading…` row.
    #[gpui::test]
    fn resolved_source_database_renders_tables_even_when_not_enumerated(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        // No snapshot, and the cached list is an authoritative EMPTY success —
        // nothing anywhere enumerates "app".
        fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, profile_id, fake.clone(), None);

        let seeded = vec![TableRef {
            schema: Some("public".to_string()),
            name: "users".to_string(),
        }];
        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    seeded.clone(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        let app_node_id = database_node_id(profile_id, "app");
        let loading_row = format!("{app_node_id}::__status");
        let users_row = table_node_id(profile_id, "app", Some("public"), "users");
        let rows = phase.read_with(cx, |phase, _| flat_row_ids(phase));

        assert!(
            !rows.iter().any(|id| id.as_ref() == loading_row),
            "a successfully loaded source database must not remain on its Loading row"
        );
        phase.update(cx, |phase, _| phase.source.tree.select_by_id(&users_row));
        assert!(
            phase.read_with(cx, |phase, _| flat_row_ids(phase).contains(&users_row)),
            "the fetched table must render under the non-enumerated source database"
        );
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.checked_source_tables()),
            seeded,
            "pre-seeded checks must resolve against the loaded database"
        );
        assert!(phase.read_with(cx, |phase, _| {
            phase
                .source
                .model
                .payload(&users_row)
                .is_some_and(|payload| super::is_source_table_checkable(Some(payload), "app"))
        }));

        let schema_key = ObjectTreeRequestKey::DatabaseSchema {
            profile_id,
            database: "app".to_string(),
        };
        assert_eq!(
            state.read_with(cx, |state, _| state
                .object_tree_outcome(&schema_key)
                .cloned()),
            Some(ObjectTreeOutcome::Applied),
        );
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&schema_key)));
    }

    /// The same lazy profile with a FAILING database list: the resolved
    /// source must stay usable — its schema load succeeds and its tables
    /// render — even though enumeration never succeeded.
    #[gpui::test]
    fn resolved_source_database_survives_a_failing_list(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        fake.fail_list_once();
        fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, profile_id, fake.clone(), None);

        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    profile_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        let list_key = ObjectTreeRequestKey::DatabaseList { profile_id };
        assert!(
            matches!(
                state.read_with(cx, |state, _| state.object_tree_outcome(&list_key).cloned()),
                Some(ObjectTreeOutcome::Failed(_))
            ),
            "the list load failed as injected"
        );

        let app_node_id = database_node_id(profile_id, "app");
        let loading_row = format!("{app_node_id}::__status");
        let users_row = table_node_id(profile_id, "app", Some("public"), "users");
        let rows = phase.read_with(cx, |phase, _| flat_row_ids(phase));
        assert!(
            !rows.iter().any(|id| id.as_ref() == loading_row),
            "a failed list must not leave the loaded source database on its Loading row"
        );
        phase.update(cx, |phase, _| phase.source.tree.select_by_id(&users_row));
        assert!(
            phase.read_with(cx, |phase, _| flat_row_ids(phase).contains(&users_row)),
            "the resolved source must stay usable when listing fails"
        );
        assert_eq!(fake.schema_calls(), vec!["app".to_string()]);
    }
    /// Blocker 1 regression: after a shared-event rebuild removed a checked
    /// table from the source projection, the stale payload must not keep it
    /// counting toward Continue readiness. Surviving checked tables and the
    /// removed table's check intent behave per the current projection.
    #[gpui::test]
    fn removed_source_tables_stop_counting_toward_readiness(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let source_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();

        let source_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        for (name, current) in [("app", true), ("archive", false)] {
            source_fake
                .databases
                .lock()
                .expect("databases")
                .push(DatabaseInfo {
                    name: name.into(),
                    is_current: current,
                });
        }
        source_fake.set_schema(
            "app",
            db_schema(
                "app",
                vec![
                    test_table(Some("public"), "users"),
                    test_table(None, "orders"),
                ],
            ),
        );
        source_fake.set_schema(
            "archive",
            db_schema("archive", vec![test_table(None, "loose")]),
        );
        connect_profile(&state, cx, source_id, source_fake.clone(), None);

        let target_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        target_fake
            .databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "w".into(),
                is_current: true,
            });
        connect_profile(&state, cx, target_id, target_fake.clone(), None);

        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    source_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        // Select the target container (the target tree is phase.target).
        let target_connection = connection_node_id(target_id);
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&target_connection);
            phase.activate_current(TreeSide::Target, cx);
        });
        cx.run_until_parked();
        let warehouse = database_node_id(target_id, "w");
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&warehouse);
            phase.activate_current(TreeSide::Target, cx);
        });
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some("w".to_string()),
        );

        // Check the source table that will disappear below.
        let users_row = table_node_id(source_id, "app", Some("public"), "users");
        let orders_row = table_node_id(source_id, "app", None, "orders");
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&users_row);
            phase.activate_current(TreeSide::Source, cx);
        });
        assert!(phase.read_with(cx, |phase, _| {
            phase
                .checked_source_tables()
                .iter()
                .any(|table| table.name == "users")
        }));
        assert!(phase.read_with(cx, |phase, app| phase.is_ready(app)));

        // The source session is replaced with a snapshot that no longer
        // contains `users`; browsing another database then forces a real
        // shared-event rebuild that drops the `users` row.
        let replacement = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        replacement.set_schema(
            "archive",
            db_schema("archive", vec![test_table(None, "loose")]),
        );
        let mut profile =
            dbflux_core::ConnectionProfile::new("test-profile", DbConfig::default_postgres());
        profile.id = source_id;
        profile.set_driver_id(DRIVER_KEY);
        state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile,
                replacement.clone() as Arc<dyn Connection>,
                Some(relational_schema(
                    vec![
                        DatabaseInfo {
                            name: "app".into(),
                            is_current: true,
                        },
                        DatabaseInfo {
                            name: "archive".into(),
                            is_current: false,
                        },
                    ],
                    Some("app"),
                    Vec::new(),
                    vec![test_table(None, "orders")],
                )),
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
        let archive_id = database_node_id(source_id, "archive");
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&archive_id);
            phase.activate_current(TreeSide::Source, cx);
        });
        cx.run_until_parked();

        // The rebuild removed the row from the tree...
        assert!(
            !phase.read_with(cx, |phase, _| flat_row_ids(phase).contains(&users_row)),
            "the removed table must disappear from the rendered tree"
        );
        // ...and must stop counting toward readiness: it no longer resolves
        // against the CURRENT projection, while the surviving checked table
        // still does.
        let checked = phase.read_with(cx, |phase, _| phase.checked_source_tables());
        assert!(
            !checked.iter().any(|table| table.name == "users"),
            "a removed table's stale payload must not stay checked/advanceable"
        );
        assert!(
            checked.is_empty(),
            "the removed table was the only checked one: no ghost selections remain"
        );
        assert!(
            !phase.read_with(cx, |phase, app| phase.is_ready(app)),
            "Continue must not stay enabled on a stale removed source table"
        );
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some("w".to_string()),
            "the untouched target selection is unaffected"
        );

        // Positive control: the surviving table is still checkable and a
        // current-projection check re-enables the advance guard.
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&orders_row);
            phase.activate_current(TreeSide::Source, cx);
        });
        let checked = phase.read_with(cx, |phase, _| phase.checked_source_tables());
        assert_eq!(checked.len(), 1);
        assert_eq!(checked[0].name, "orders");
        assert!(phase.read_with(cx, |phase, app| phase.is_ready(app)));
    }
    /// Blocker 2 regression: a selected target container that an
    /// authoritative target-list refresh no longer offers must stop being
    /// the effective selection (and notify the host), while an unloaded or
    /// merely refetching profile must NOT invalidate a retained selection.
    #[gpui::test]
    fn removed_target_container_clears_the_selection_and_notifies(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let source_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();

        let source_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        source_fake
            .databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "app".into(),
                is_current: true,
            });
        source_fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, source_id, source_fake.clone(), None);

        let target_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        for name in ["w", "z"] {
            target_fake
                .databases
                .lock()
                .expect("databases")
                .push(DatabaseInfo {
                    name: name.into(),
                    is_current: name == "w",
                });
        }
        connect_profile(&state, cx, target_id, target_fake.clone(), None);

        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    source_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        // Count SourceTargetChanged emissions the host would observe.
        let emissions: Arc<Mutex<Vec<()>>> = Arc::new(Mutex::new(Vec::new()));
        struct NotifyMarker {
            _subscription: gpui::Subscription,
        }
        impl Render for NotifyMarker {
            fn render(
                &mut self,
                _window: &mut gpui::Window,
                _cx: &mut gpui::Context<Self>,
            ) -> impl IntoElement {
                div()
            }
        }
        let sink = emissions.clone();
        let observed = phase.clone();
        let _notify_marker = cx.update(|cx| {
            cx.new(|cx| NotifyMarker {
                _subscription: cx.subscribe(
                    &observed,
                    move |_entity, _phase, _event: &super::SourceTargetChanged, _cx| {
                        sink.lock().expect("emissions").push(());
                    },
                ),
            })
        });

        // Expand the target connection and select "w".
        let target_connection = connection_node_id(target_id);
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&target_connection);
            phase.activate_current(TreeSide::Target, cx);
        });
        cx.run_until_parked();
        let w_leaf = database_node_id(target_id, "w");
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&w_leaf);
            phase.activate_current(TreeSide::Target, cx);
        });
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some("w".to_string()),
        );

        // Check a source table so readiness is otherwise satisfied.
        let users_row = table_node_id(source_id, "app", Some("public"), "users");
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&users_row);
            phase.activate_current(TreeSide::Source, cx);
        });
        assert!(phase.read_with(cx, |phase, app| phase.is_ready(app)));
        let emissions_before_refresh = emissions.lock().expect("emissions").len();

        // The target session is replaced with one whose authoritative list no
        // longer offers "w" (profile stays connected and compatible).
        let replacement = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        replacement
            .databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "z".into(),
                is_current: true,
            });
        let mut profile =
            dbflux_core::ConnectionProfile::new("test-profile", DbConfig::default_postgres());
        profile.id = target_id;
        profile.set_driver_id(DRIVER_KEY);
        state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile,
                replacement.clone() as Arc<dyn Connection>,
                None,
                None,
                false,
                WritePrivilege::Unknown,
            );
        });

        // A first shared event rebuilds while the target database set is
        // merely UNLOADED: nothing has proven "w" gone, so the selection is
        // retained.
        let cached_schema = ObjectTreeRequestKey::DatabaseSchema {
            profile_id: source_id,
            database: "app".to_string(),
        };
        state.update(cx, |state, cx| {
            state.object_tree_request(cached_schema, cx);
        });
        cx.run_until_parked();
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some("w".to_string()),
            "an unloaded target database set must not invalidate the selection"
        );

        // Refreshing the target list authoritatively removes "w".
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&target_connection);
            phase.activate_current(TreeSide::Target, cx);
            phase.target.tree.select_by_id(&target_connection);
            phase.activate_current(TreeSide::Target, cx);
        });
        cx.run_until_parked();

        assert!(
            !phase.read_with(cx, |phase, _| target_flat_row_ids(phase).contains(&w_leaf)),
            "the removed container must disappear from the rendered tree"
        );
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            None,
            "a removed target container must not stay the effective selection"
        );
        assert!(
            !phase.read_with(cx, |phase, app| phase.is_ready(app)),
            "Continue must not stay enabled on a removed target container"
        );
        assert!(
            emissions.lock().expect("emissions").len() > emissions_before_refresh,
            "the host must be notified when the effective selection is cleared"
        );

        // The surviving container is still selectable and enables the guard.
        let z_leaf = database_node_id(target_id, "z");
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&z_leaf);
            phase.activate_current(TreeSide::Target, cx);
        });
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some("z".to_string()),
        );
        assert!(phase.read_with(cx, |phase, app| phase.is_ready(app)));
    }
    /// Blocker 3 regression: an authoritatively empty single-database target
    /// (successful empty list, no snapshot, no active database name) must
    /// still offer its implicit container for selection — never a perpetual
    /// `Loading…` row masquerading on a settled-empty profile.
    #[gpui::test]
    fn authoritatively_empty_target_gets_a_selectable_implicit_container(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let source_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();

        let source_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        source_fake
            .databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "app".into(),
                is_current: true,
            });
        source_fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, source_id, source_fake.clone(), None);

        // Empty single-database target: successful EMPTY list, no snapshot,
        // no active database name — the implicit identity is the empty name.
        // Empty single-database target: successful EMPTY list, no snapshot,
        // no active database name — the implicit identity is the empty name.
        let target_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        connect_profile(&state, cx, target_id, target_fake.clone(), None);

        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    source_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        // Count SourceTargetChanged emissions the host would observe.
        let emissions: Arc<Mutex<Vec<()>>> = Arc::default();
        struct NotifyMarker {
            _subscription: gpui::Subscription,
        }
        impl Render for NotifyMarker {
            fn render(
                &mut self,
                _window: &mut gpui::Window,
                _cx: &mut gpui::Context<Self>,
            ) -> impl IntoElement {
                div()
            }
        }
        let sink = emissions.clone();
        let observed = phase.clone();
        let _notify_marker = cx.update(|cx| {
            cx.new(|cx| NotifyMarker {
                _subscription: cx.subscribe(
                    &observed,
                    move |_entity, _phase, _event: &super::SourceTargetChanged, _cx| {
                        sink.lock().expect("emissions").push(());
                    },
                ),
            })
        });

        let target_connection = connection_node_id(target_id);
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&target_connection);
            phase.activate_current(TreeSide::Target, cx);
        });
        cx.run_until_parked();

        let rows = phase.read_with(cx, |phase, _| target_flat_row_ids(phase));
        let loading_row = format!("{target_connection}::__status");
        assert!(
            !rows.iter().any(|id| id.as_ref() == loading_row),
            "a settled-empty target profile must not masquerade as loading"
        );
        let implicit_leaf = database_node_id(target_id, "");
        assert!(
            rows.contains(&implicit_leaf),
            "an authoritatively empty target must offer its implicit container"
        );
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&implicit_leaf);
            phase.activate_current(TreeSide::Target, cx);
        });
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some(String::new()),
            "the empty-identity implicit container must be selectable"
        );

        // A checked source plus the implicit target satisfies the guard.
        let users_row = table_node_id(source_id, "app", Some("public"), "users");
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&users_row);
            phase.activate_current(TreeSide::Source, cx);
        });
        assert!(phase.read_with(cx, |phase, app| phase.is_ready(app)));

        // A later shared settle rebuilds the target tree. The implicit
        // container is still offered by the same adapted projection, so the
        // selection must survive with NO spurious host change event and
        // readiness intact.
        let emissions_before = emissions.lock().expect("emissions").len();
        // Trigger: a shared database-list settle for the source profile. It
        // rebuilds the target tree without the resolved-source recheck
        // emission, so any SourceTargetChanged here would be spurious.
        let cached_list = ObjectTreeRequestKey::DatabaseList {
            profile_id: source_id,
        };
        state.update(cx, |state, cx| {
            state.object_tree_request(cached_list, cx);
        });
        cx.run_until_parked();

        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some(String::new()),
            "a valid implicit-fallback selection must survive shared settles"
        );
        assert_eq!(
            emissions.lock().expect("emissions").len(),
            emissions_before,
            "no spurious host change event for a retained valid selection"
        );
        assert!(phase.read_with(cx, |phase, app| phase.is_ready(app)));
    }

    /// A non-empty active-database identity names the implicit target.
    #[gpui::test]
    fn non_empty_active_database_names_the_implicit_target(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let source_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();

        let source_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        source_fake
            .databases
            .lock()
            .expect("databases")
            .push(DatabaseInfo {
                name: "app".into(),
                is_current: true,
            });
        source_fake.set_schema(
            "app",
            db_schema("app", vec![test_table(Some("public"), "users")]),
        );
        connect_profile(&state, cx, source_id, source_fake.clone(), None);

        let target_fake = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        target_fake.set_active_database("maindb");
        connect_profile(&state, cx, target_id, target_fake.clone(), None);

        let phase = cx.update(|cx| {
            cx.new(|cx| {
                SourceTargetPhase::new(
                    state.clone(),
                    source_id,
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                )
            })
        });
        cx.run_until_parked();

        // Count SourceTargetChanged emissions the host would observe.
        let emissions: Arc<Mutex<Vec<()>>> = Arc::default();
        struct NotifyMarker {
            _subscription: gpui::Subscription,
        }
        impl Render for NotifyMarker {
            fn render(
                &mut self,
                _window: &mut gpui::Window,
                _cx: &mut gpui::Context<Self>,
            ) -> impl IntoElement {
                div()
            }
        }
        let sink = emissions.clone();
        let observed = phase.clone();
        let _notify_marker = cx.update(|cx| {
            cx.new(|cx| NotifyMarker {
                _subscription: cx.subscribe(
                    &observed,
                    move |_entity, _phase, _event: &super::SourceTargetChanged, _cx| {
                        sink.lock().expect("emissions").push(());
                    },
                ),
            })
        });

        let target_connection = connection_node_id(target_id);
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&target_connection);
            phase.activate_current(TreeSide::Target, cx);
        });
        cx.run_until_parked();

        let implicit_leaf = database_node_id(target_id, "maindb");
        let rows = phase.read_with(cx, |phase, _| target_flat_row_ids(phase));
        assert!(
            rows.contains(&implicit_leaf),
            "the implicit container must carry the connection's active database identity"
        );
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&implicit_leaf);
            phase.activate_current(TreeSide::Target, cx);
        });
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some("maindb".to_string()),
        );

        // A checked source table makes readiness otherwise satisfiable, so
        // readiness retention is observable below.
        let users_row = table_node_id(source_id, "app", Some("public"), "users");
        phase.update(cx, |phase, cx| {
            phase.source.tree.select_by_id(&users_row);
            phase.activate_current(TreeSide::Source, cx);
        });

        // A later shared settle rebuilds the target tree; the still-offered
        // implicit container must keep the selection valid without a
        // spurious host change event.
        let emissions_before = emissions.lock().expect("emissions").len();
        // Trigger: a shared database-list settle for the source profile. It
        // rebuilds the target tree without the resolved-source recheck
        // emission, so any SourceTargetChanged here would be spurious.
        let cached_list = ObjectTreeRequestKey::DatabaseList {
            profile_id: source_id,
        };
        state.update(cx, |state, cx| {
            state.object_tree_request(cached_list, cx);
        });
        cx.run_until_parked();
        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            Some("maindb".to_string()),
            "a valid implicit-fallback selection must survive shared settles"
        );
        assert_eq!(
            emissions.lock().expect("emissions").len(),
            emissions_before,
            "no spurious host change event for a retained valid selection"
        );
        assert!(phase.read_with(cx, |phase, app| phase.is_ready(app)));

        // Current identity change invalidates the old fallback: the target
        // connection is replaced with one whose active database is unnamed,
        // and the refreshed authoritative list re-derives the implicit
        // identity as the empty name. The old "maindb" selection must clear
        // and notify, while the new empty-identity container appears.
        let replacement = PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
        let mut profile = dbflux_core::ConnectionProfile::new(
            "test-profile",
            dbflux_core::DbConfig::default_postgres(),
        );
        profile.id = target_id;
        profile.set_driver_id(DRIVER_KEY);
        state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile,
                replacement.clone() as Arc<dyn Connection>,
                None,
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
        phase.update(cx, |phase, cx| {
            phase.target.tree.select_by_id(&target_connection);
            phase.activate_current(TreeSide::Target, cx);
            phase.target.tree.select_by_id(&target_connection);
            phase.activate_current(TreeSide::Target, cx);
        });
        cx.run_until_parked();

        assert_eq!(
            phase.read_with(cx, |phase, _| phase.target_database()),
            None,
            "an old fallback identity the current connection no longer derives must clear"
        );
        assert!(
            emissions.lock().expect("emissions").len() > emissions_before,
            "the host must be notified when the fallback identity change clears the selection"
        );
        let rows = phase.read_with(cx, |phase, _| target_flat_row_ids(phase));
        assert!(rows.contains(&database_node_id(target_id, "")));
        assert!(!rows.contains(&implicit_leaf));
    }
}
