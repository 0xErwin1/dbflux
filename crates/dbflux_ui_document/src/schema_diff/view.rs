//! `SchemaDiffDocument` — the schema-diff & apply document.
//!
//! Resolves two schema sources (live-to-live by default, snapshot-to-live as a
//! secondary mode), renders the per-table diff with a three-level risk badge and
//! selection checkboxes, previews the driver-generated DDL read-only, and applies
//! the selected changes through `DdlApplyExecutor` behind a hard-confirm gate.
//! Changes the driver cannot express are surfaced explicitly, never dropped.

use std::collections::HashSet;
use std::sync::Arc;

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::common::time_range::{TimestampDisplayMode, format_timestamp_ms};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{
    ModalMutationConfirmHard, MutationConfirmHardRequest, MutationConfirmOutcome,
};
use dbflux_components::primitives::{Badge, BadgeVariant, Icon, Text};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    Connection, ExecutionClassification, MutationPolicy, QueryLanguage, RefreshPolicy,
    RiskedChange, SchemaChange, TableInfo, TableRef, diff_schema,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::sql_preview_modal::SqlPreviewModal;
use dbflux_ui_base::toast::{PendingToast, flush_pending_toast};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

use super::apply::{
    DdlApplyDeps, DdlApplyExecutor, DdlApplyOutcome, TableLevelAction,
    build_statements_for_table_action,
};
use super::diff_source::{
    DiffMode, PartitionedChanges, ReferenceTarget, RiskBadge, SourcePicker, TableActionOutcome,
    UnsupportedChange, classify_table_action, live_reference_ready, partition_table_changes,
    resolve_same_connection_shallow, same_connection_reference_databases,
};
use crate::handle::DocumentEvent;
use crate::types::{DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot, DocumentState};

/// One table's slice of the diff, grouped for rendering.
struct TableDiffGroup {
    table: TableRef,
    /// Human header, e.g. "public.users".
    header: String,
    /// Changes the driver can apply, with a stable index used for selection.
    applicable: Vec<RiskedChange>,
    /// Changes surfaced explicitly as unsupported (never applied).
    unsupported: Vec<UnsupportedChange>,
    /// Present for whole-table add/remove (`TableChange::TableAdded`/
    /// `TableRemoved`), which carry no per-column `RiskedChange` of their own.
    table_action: Option<TableActionOutcome>,
}

impl TableDiffGroup {
    fn is_empty(&self) -> bool {
        self.applicable.is_empty() && self.unsupported.is_empty() && self.table_action.is_none()
    }
}

/// One table's selected work: individual column/index changes plus, if
/// selected, the whole-table action and its risk (for approval-routing
/// classification).
#[derive(Clone)]
struct SelectedTableWork {
    table: TableRef,
    changes: Vec<RiskedChange>,
    table_action: Option<(TableLevelAction, ExecutionClassification)>,
}

/// Explicit lifecycle of the diff computation. Separates a genuine failure
/// (`Error`) from a clean, successful comparison that simply found no
/// differences (`Empty`); the two were previously conflated behind a single
/// `status_message`, which made an identical-schema result render as an error.
enum ComputeState {
    /// No comparison has run yet.
    Idle,
    /// A comparison (or an apply that re-runs the comparison) is in progress.
    Loading,
    /// The comparison failed with a fatal error to surface to the user.
    Error(String),
    /// The comparison succeeded and found no differences.
    Empty,
    /// The comparison succeeded and produced at least one diff group.
    Diff,
}

/// Maps the compute lifecycle onto the document chrome state. A clean no-diff
/// result (`Empty`) is a healthy `Clean` state — NOT an error — which is the
/// whole point of separating `Empty` from `Error`.
fn document_state_for(state: &ComputeState) -> DocumentState {
    match state {
        ComputeState::Loading => DocumentState::Loading,
        ComputeState::Error(_) => DocumentState::Error,
        ComputeState::Idle | ComputeState::Empty | ComputeState::Diff => DocumentState::Clean,
    }
}

/// Successful multi-table apply summary (FIX-6 aggregate reporting).
struct ApplyRunOutcome {
    statements_applied: usize,
    tables_applied: usize,
}

/// Multi-table apply failure carrying the running progress so the user learns
/// how many tables committed before the stop and which one failed.
struct ApplyRunFailure {
    failed_table: String,
    tables_applied: usize,
    statements_applied: usize,
    message: String,
}

/// The schema-diff & apply document entity.
pub struct SchemaDiffDocument {
    id: DocumentId,
    app_state: Entity<AppStateEntity>,

    /// Live target: the connection DDL is applied to. This side is `before` in
    /// the diff, so changes describe how to transform the target into the
    /// reference schema.
    profile_id: Uuid,
    database: Option<String>,
    title: String,

    picker: SourcePicker,
    /// Reference side chosen in `LiveVsLive` mode: either another database on
    /// this same connection or a different open relational connection.
    reference: Option<ReferenceTarget>,
    /// Database names known on the target's own connection, used to offer
    /// same-connection reference databases. Loaded off the UI thread.
    connection_databases: Vec<String>,
    groups: Vec<TableDiffGroup>,
    /// Selected applicable changes as `(group_index, applicable_index)`.
    selected: HashSet<(usize, usize)>,
    /// Selected whole-table actions, as `group_index`.
    selected_table_actions: HashSet<usize>,
    /// Snapshot summaries for the target profile/database, loaded when the
    /// snapshot-to-live mode is selected.
    snapshots: Vec<dbflux_storage::repositories::sch_schema_snapshots::SchemaSnapshotSummary>,

    compute_state: ComputeState,
    pending_toast: Option<PendingToast>,

    sql_preview_modal: Entity<SqlPreviewModal>,
    confirm_modal: Entity<ModalMutationConfirmHard>,

    pending_preview: Option<String>,
    pending_confirm: Option<MutationConfirmHardRequest>,
    pending_apply: bool,

    focus_handle: FocusHandle,
    diff_scroll: ScrollHandle,
    _subscriptions: Vec<Subscription>,
}

impl SchemaDiffDocument {
    pub fn new(
        profile_id: Uuid,
        database: Option<String>,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let sql_preview_modal = cx.new(|cx| SqlPreviewModal::new(app_state.clone(), window, cx));
        let confirm_modal = cx.new(|cx| ModalMutationConfirmHard::new(window, cx));

        let confirm_sub = cx.subscribe(
            &confirm_modal,
            |this, _modal, outcome: &MutationConfirmOutcome, cx| {
                this.on_confirm_outcome(outcome.clone(), cx);
            },
        );

        let title = match &database {
            Some(db) => format!("Schema Diff — {db}"),
            None => "Schema Diff".to_string(),
        };

        let mut document = Self {
            id: DocumentId::new(),
            app_state,
            profile_id,
            database,
            title,
            picker: SourcePicker::default(),
            reference: None,
            connection_databases: Vec::new(),
            groups: Vec::new(),
            selected: HashSet::new(),
            selected_table_actions: HashSet::new(),
            snapshots: Vec::new(),
            compute_state: ComputeState::Idle,
            pending_toast: None,
            sql_preview_modal,
            confirm_modal,
            pending_preview: None,
            pending_confirm: None,
            pending_apply: false,
            focus_handle: cx.focus_handle(),
            diff_scroll: ScrollHandle::new(),
            _subscriptions: vec![confirm_sub],
        };

        document.load_connection_databases(cx);
        document
    }

    /// Loads the target connection's database names off the UI thread so the
    /// picker can offer same-connection reference databases. Enriches the
    /// already-cached database names with the full server listing; a listing
    /// failure degrades gracefully to whatever is cached rather than surfacing
    /// an error, since this only populates a picker. When nothing is selected
    /// yet and the connection has more than one database, the first other
    /// database is pre-selected — the common comparison case.
    fn load_connection_databases(&mut self, cx: &mut Context<Self>) {
        let Some(connected) = self.app_state.read(cx).connections().get(&self.profile_id) else {
            return;
        };

        let connection = Arc::clone(&connected.connection);
        let mut baseline: Vec<String> = connected.database_schemas.keys().cloned().collect();
        if let Some(active) = &connected.active_database
            && !baseline.contains(active)
        {
            baseline.push(active.clone());
        }

        let task = cx.background_executor().spawn(async move {
            connection
                .list_databases()
                .map(|dbs| dbs.into_iter().map(|d| d.name).collect::<Vec<String>>())
        });

        cx.spawn(async move |this, cx| {
            let listed = task.await;
            cx.update(|cx| {
                this.update(cx, |doc, cx| {
                    let mut names = baseline;
                    if let Ok(listed) = listed {
                        for name in listed {
                            if !names.contains(&name) {
                                names.push(name);
                            }
                        }
                    }
                    names.sort();
                    names.dedup();
                    doc.connection_databases = names;

                    if doc.reference.is_none() {
                        let candidates = same_connection_reference_databases(
                            &doc.connection_databases,
                            doc.database.as_deref(),
                        );
                        if let Some(first) = candidates.into_iter().next() {
                            doc.reference = Some(ReferenceTarget::SameConnectionDatabase(first));
                        }
                    }

                    cx.notify();
                })
            })
            .ok();
        })
        .detach();
    }

    // ── Document API (mirrored by the pane) ───────────────────────────────

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn state(&self) -> DocumentState {
        document_state_for(&self.compute_state)
    }

    fn is_busy(&self) -> bool {
        matches!(self.compute_state, ComputeState::Loading)
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        Some(self.profile_id)
    }

    pub fn active_context(&self) -> ContextId {
        ContextId::Global
    }

    pub fn current_refresh_policy(&self) -> RefreshPolicy {
        RefreshPolicy::Manual
    }

    pub fn apply_refresh_policy(&mut self, _policy: RefreshPolicy, _cx: &mut Context<Self>) {}

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub fn dispatch_command(
        &mut self,
        _cmd: Command,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> bool {
        false
    }

    /// Dedup match: same target profile + database.
    pub fn matches_schema_diff(&self, profile_id: Uuid, database: Option<&str>) -> bool {
        self.profile_id == profile_id && self.database.as_deref() == database
    }

    // ── Source picker ─────────────────────────────────────────────────────

    fn set_mode(&mut self, mode: DiffMode, cx: &mut Context<Self>) {
        if self.picker.mode == mode {
            return;
        }
        self.picker.mode = mode;
        self.compute_state = ComputeState::Idle;
        self.groups.clear();
        self.selected.clear();
        self.selected_table_actions.clear();
        match mode {
            DiffMode::SnapshotVsLive => self.load_snapshots(cx),
            DiffMode::LiveVsLive => {
                if self.connection_databases.is_empty() {
                    self.load_connection_databases(cx);
                }
            }
        }
        cx.notify();
    }

    fn load_snapshots(&mut self, cx: &mut Context<Self>) {
        let profile_id = self.profile_id.to_string();
        let database = self.database.clone();
        let snapshots = self.app_state.update(cx, |state, _| {
            state
                .schema_snapshots
                .list(&profile_id, database.as_deref())
        });
        self.snapshots = snapshots;
    }

    fn select_snapshot(&mut self, snapshot_id: Uuid, cx: &mut Context<Self>) {
        self.picker.selected_snapshot = Some(snapshot_id);
        self.compute_state = ComputeState::Idle;
        self.groups.clear();
        self.selected.clear();
        self.selected_table_actions.clear();
        cx.notify();
    }

    /// Selects another database on the target's own connection as the reference.
    fn select_same_connection_database(&mut self, database: String, cx: &mut Context<Self>) {
        self.picker.mode = DiffMode::LiveVsLive;
        self.reference = Some(ReferenceTarget::SameConnectionDatabase(database));
        self.reset_after_reference_change();
        cx.notify();
    }

    /// Selects a different open relational connection as the reference.
    fn select_reference_connection(&mut self, other_profile_id: Uuid, cx: &mut Context<Self>) {
        self.picker.mode = DiffMode::LiveVsLive;
        self.reference = Some(ReferenceTarget::OtherConnection {
            profile_id: other_profile_id,
            database: None,
        });
        self.reset_after_reference_change();
        cx.notify();
    }

    fn reset_after_reference_change(&mut self) {
        self.compute_state = ComputeState::Idle;
        self.groups.clear();
        self.selected.clear();
        self.selected_table_actions.clear();
    }

    // ── Diff computation ──────────────────────────────────────────────────

    fn compute_diff(&mut self, cx: &mut Context<Self>) {
        let reference = self.reference.clone();
        let state = self.app_state.read(cx);

        let Some(target) = state.connections().get(&self.profile_id) else {
            self.compute_state =
                ComputeState::Error("Target connection is no longer available.".to_string());
            cx.notify();
            return;
        };

        let target_connection = Arc::clone(&target.connection);
        let target_shallow: Vec<TableInfo> = target
            .schema
            .as_ref()
            .map(|s| s.tables().to_vec())
            .unwrap_or_default();
        let target_db = self.database.clone();

        // Resolve the reference side into a Send-friendly plan.
        let reference_plan = match self.picker.mode {
            DiffMode::LiveVsLive => match &reference {
                None => {
                    self.compute_state = ComputeState::Error(
                        "Pick a reference database or connection to compare against.".to_string(),
                    );
                    cx.notify();
                    return;
                }
                Some(ReferenceTarget::SameConnectionDatabase(db)) => {
                    // Same connection, different database: reuse the target
                    // connection Arc and pull the reference database's cached
                    // shallow tables. A database whose schema is not loaded yet
                    // is refused rather than compared as empty, which would
                    // otherwise read as dropping every table.
                    match resolve_same_connection_shallow(&target.database_schemas, db) {
                        Ok(shallow) => SidePlan::Live {
                            connection: Arc::clone(&target_connection),
                            database: Some(db.clone()),
                            shallow,
                        },
                        Err(message) => {
                            self.compute_state = ComputeState::Error(message);
                            cx.notify();
                            return;
                        }
                    }
                }
                Some(ReferenceTarget::OtherConnection {
                    profile_id,
                    database,
                }) => {
                    let Some(other) = state.connections().get(profile_id) else {
                        self.compute_state = ComputeState::Error(
                            "The chosen reference connection is not connected.".to_string(),
                        );
                        cx.notify();
                        return;
                    };
                    let other_db = database.clone().or_else(|| other.active_database.clone());
                    let other_shallow = other
                        .schema
                        .as_ref()
                        .map(|s| s.tables().to_vec())
                        .unwrap_or_default();
                    SidePlan::Live {
                        connection: Arc::clone(&other.connection),
                        database: other_db,
                        shallow: other_shallow,
                    }
                }
            },
            DiffMode::SnapshotVsLive => {
                let Some(snapshot_id) = self.picker.selected_snapshot else {
                    self.compute_state =
                        ComputeState::Error("Pick a snapshot to compare against.".to_string());
                    cx.notify();
                    return;
                };
                match state.schema_snapshots.get(&snapshot_id.to_string()) {
                    Ok(Some(record)) => SidePlan::Resolved(record.tables),
                    Ok(None) => {
                        self.compute_state =
                            ComputeState::Error("Selected snapshot no longer exists.".to_string());
                        cx.notify();
                        return;
                    }
                    Err(e) => {
                        self.compute_state =
                            ComputeState::Error(format!("Failed to load snapshot: {e}"));
                        cx.notify();
                        return;
                    }
                }
            }
        };

        self.compute_state = ComputeState::Loading;
        cx.notify();

        let target_db_for_task = target_db.clone();

        let task = cx.background_executor().spawn(async move {
            // `before` = target live (DDL applies here); `after` = reference.
            // A failed `table_details` on EITHER side aborts the comparison
            // with a clear error instead of silently degrading to a
            // column-less entry, which would produce a wrong/destructive diff.
            let before = deep_resolve(
                &*target_connection,
                target_db_for_task.as_deref(),
                &target_shallow,
            )?;
            let after = match reference_plan {
                SidePlan::Live {
                    connection,
                    database,
                    shallow,
                } => deep_resolve(&*connection, database.as_deref(), &shallow)?,
                SidePlan::Resolved(tables) => tables,
            };

            let table_changes = diff_schema(&before, &after);
            Ok::<Vec<TableDiffGroup>, String>(build_groups(&*target_connection, table_changes))
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;
            cx.update(|cx| {
                this.update(cx, |doc, cx| {
                    match result {
                        Ok(groups) => {
                            doc.groups = groups;
                            doc.selected = doc.default_selection();
                            doc.selected_table_actions = doc.default_table_action_selection();
                            doc.compute_state = if doc.groups.is_empty() {
                                ComputeState::Empty
                            } else {
                                ComputeState::Diff
                            };
                        }
                        Err(message) => {
                            doc.groups.clear();
                            doc.selected.clear();
                            doc.selected_table_actions.clear();
                            doc.compute_state = ComputeState::Error(message.clone());
                            report_error(UserFacingError::new(ErrorKind::Driver, message), cx);
                        }
                    }
                    cx.notify();
                })
            })
            .ok();
        })
        .detach();
    }

    /// Default selection = every applicable change checked.
    fn default_selection(&self) -> HashSet<(usize, usize)> {
        let mut set = HashSet::new();
        for (group_index, group) in self.groups.iter().enumerate() {
            for change_index in 0..group.applicable.len() {
                set.insert((group_index, change_index));
            }
        }
        set
    }

    /// Default selection = every applicable whole-table action checked,
    /// mirroring `default_selection`'s "select every applicable change" rule.
    fn default_table_action_selection(&self) -> HashSet<usize> {
        self.groups
            .iter()
            .enumerate()
            .filter_map(|(index, group)| {
                matches!(
                    group.table_action,
                    Some(TableActionOutcome::Applicable { .. })
                )
                .then_some(index)
            })
            .collect()
    }

    fn toggle_selection(
        &mut self,
        group_index: usize,
        change_index: usize,
        cx: &mut Context<Self>,
    ) {
        let key = (group_index, change_index);
        if !self.selected.remove(&key) {
            self.selected.insert(key);
        }
        cx.notify();
    }

    fn toggle_table_action_selection(&mut self, group_index: usize, cx: &mut Context<Self>) {
        if !self.selected_table_actions.remove(&group_index) {
            self.selected_table_actions.insert(group_index);
        }
        cx.notify();
    }

    /// Collects the selected applicable changes and whole-table actions,
    /// grouped by table.
    fn selected_changes_by_table(&self) -> Vec<SelectedTableWork> {
        let mut out: Vec<SelectedTableWork> = Vec::new();

        for (group_index, group) in self.groups.iter().enumerate() {
            let mut picked = Vec::new();
            for (change_index, change) in group.applicable.iter().enumerate() {
                if self.selected.contains(&(group_index, change_index)) {
                    picked.push(change.clone());
                }
            }

            let table_action = if self.selected_table_actions.contains(&group_index) {
                match &group.table_action {
                    Some(TableActionOutcome::Applicable { action, risk }) => {
                        Some((action.clone(), *risk))
                    }
                    _ => None,
                }
            } else {
                None
            };

            if !picked.is_empty() || table_action.is_some() {
                out.push(SelectedTableWork {
                    table: group.table.clone(),
                    changes: picked,
                    table_action,
                });
            }
        }

        out
    }

    fn has_destructive_selection(&self) -> bool {
        for (group_index, group) in self.groups.iter().enumerate() {
            for (change_index, change) in group.applicable.iter().enumerate() {
                if self.selected.contains(&(group_index, change_index))
                    && RiskBadge::from_classification(change.risk) == RiskBadge::Destructive
                {
                    return true;
                }
            }
            if self.selected_table_actions.contains(&group_index)
                && let Some(TableActionOutcome::Applicable { risk, .. }) = &group.table_action
                && RiskBadge::from_classification(*risk) == RiskBadge::Destructive
            {
                return true;
            }
        }
        false
    }

    // ── Preview ───────────────────────────────────────────────────────────

    /// Builds the joined DDL string for the current selection, running the same
    /// generation seam the apply path uses. Shared by both the preview surface
    /// and the confirm-dialog body so the two can never drift in how they
    /// build or error on the SQL.
    fn build_selected_sql(&self, cx: &Context<Self>) -> Result<String, String> {
        let selected = self.selected_changes_by_table();
        if selected.is_empty() {
            return Err("Select at least one change first.".to_string());
        }

        let Some(connection) = self.app_state.read(cx).get_connection(self.profile_id) else {
            return Err("Target connection is no longer available.".to_string());
        };

        let mut statements: Vec<String> = Vec::new();
        for work in selected {
            let executor = build_executor_for_work(
                work,
                DdlApplyDeps {
                    connection: Arc::clone(&connection),
                    event_sink: None,
                    policy: MutationPolicy::Allowed,
                },
            );
            let stmts = executor
                .preview_statements()
                .map_err(|e| format!("Cannot build DDL: {e}"))?;
            statements.extend(stmts);
        }

        Ok(statements.join(";\n\n") + ";")
    }

    fn open_preview(&mut self, cx: &mut Context<Self>) {
        match self.build_selected_sql(cx) {
            Ok(sql) => self.pending_preview = Some(sql),
            Err(message) => {
                self.pending_toast = Some(PendingToast {
                    message,
                    is_error: true,
                });
            }
        }
        cx.notify();
    }

    // ── Apply (hard-confirm gated) ────────────────────────────────────────

    fn request_apply(&mut self, cx: &mut Context<Self>) {
        let selected = self.selected_changes_by_table();
        if selected.is_empty() {
            self.pending_toast = Some(PendingToast {
                message: "Select at least one change to apply.".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        let total: usize = selected
            .iter()
            .map(|w| w.changes.len() + w.table_action.is_some() as usize)
            .sum();
        let summary = format!(
            "Apply {total} schema change(s) to {}",
            self.database.as_deref().unwrap_or("this connection")
        );

        // Build a read-only DDL preview string for the confirm body through the
        // same helper the preview surface uses; if it cannot even be generated,
        // refuse to open the confirm dialog rather than applying blind.
        let sql_preview = match self.build_selected_sql(cx) {
            Ok(sql) => sql,
            Err(message) => {
                self.pending_toast = Some(PendingToast {
                    message,
                    is_error: true,
                });
                cx.notify();
                return;
            }
        };

        self.pending_confirm = Some(MutationConfirmHardRequest {
            summary,
            type_to_confirm: "APPLY".to_string(),
            sql_preview,
            sample_rows: None,
            sample_columns: Vec::new(),
            require_opt_in: self.has_destructive_selection(),
        });
        cx.notify();
    }

    fn on_confirm_outcome(&mut self, outcome: MutationConfirmOutcome, cx: &mut Context<Self>) {
        if matches!(outcome, MutationConfirmOutcome::Cancelled) {
            return;
        }
        self.pending_apply = true;
        cx.notify();
    }

    fn run_apply(&mut self, cx: &mut Context<Self>) {
        let selected = self.selected_changes_by_table();
        if selected.is_empty() {
            return;
        }

        let (connection, event_sink, policy) = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&self.profile_id) else {
                self.pending_toast = Some(PendingToast {
                    message: "Target connection is no longer available.".to_string(),
                    is_error: true,
                });
                cx.notify();
                return;
            };
            let connection = Arc::clone(&connected.connection);
            let event_sink: Option<Arc<dyn dbflux_core::EventSink>> =
                Some(Arc::new(state.audit_service().clone()) as Arc<dyn dbflux_core::EventSink>);
            (connection, event_sink, connected.mutation_policy)
        };

        if matches!(policy, MutationPolicy::ApprovalRequired) {
            self.route_to_approval(&selected, cx);
            return;
        }

        if matches!(policy, MutationPolicy::ReadOnly) {
            self.pending_toast = Some(PendingToast {
                message: "This connection is read-only. Schema changes are not allowed."
                    .to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        self.compute_state = ComputeState::Loading;
        cx.notify();

        let total_tables = selected.len();

        let task = cx.background_executor().spawn(async move {
            let mut statements_applied = 0usize;
            let mut tables_applied = 0usize;

            for work in selected {
                let table_label = qualified(&work.table);
                let executor = build_executor_for_work(
                    work,
                    DdlApplyDeps {
                        connection: Arc::clone(&connection),
                        event_sink: event_sink.clone(),
                        policy,
                    },
                );
                match executor.apply() {
                    Ok(DdlApplyOutcome::Success {
                        statements_executed,
                        ..
                    }) => {
                        statements_applied += statements_executed;
                        tables_applied += 1;
                    }
                    Ok(other) => {
                        return Err(ApplyRunFailure {
                            failed_table: table_label,
                            tables_applied,
                            statements_applied,
                            message: format!("apply stopped: {other:?}"),
                        });
                    }
                    Err(e) => {
                        return Err(ApplyRunFailure {
                            failed_table: table_label,
                            tables_applied,
                            statements_applied,
                            message: e.to_string(),
                        });
                    }
                }
            }

            Ok(ApplyRunOutcome {
                statements_applied,
                tables_applied,
            })
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;
            cx.update(|cx| {
                this.update(cx, |doc, cx| {
                    match result {
                        Ok(outcome) => {
                            doc.pending_toast = Some(PendingToast {
                                message: format!(
                                    "Applied {} DDL statement(s) across {} table(s).",
                                    outcome.statements_applied, outcome.tables_applied
                                ),
                                is_error: false,
                            });
                            // Re-run the diff so the list reflects the new state.
                            doc.compute_state = ComputeState::Idle;
                            doc.groups.clear();
                            doc.selected.clear();
                            doc.selected_table_actions.clear();
                        }
                        Err(failure) => {
                            let not_attempted = total_tables
                                .saturating_sub(failure.tables_applied + 1);
                            let message = format!(
                                "Applied {} of {} table(s) ({} DDL statement(s)) before failing on {}: {}. \
                                 {} table(s) were not attempted.",
                                failure.tables_applied,
                                total_tables,
                                failure.statements_applied,
                                failure.failed_table,
                                failure.message,
                                not_attempted
                            );
                            // Keep the current diff visible so the user can retry
                            // the tables that did not apply.
                            doc.compute_state = if doc.groups.is_empty() {
                                ComputeState::Empty
                            } else {
                                ComputeState::Diff
                            };
                            report_error(UserFacingError::new(ErrorKind::Driver, message), cx);
                        }
                    }
                    cx.notify();
                })
            })
            .ok();
        })
        .detach();
    }

    #[cfg(feature = "mcp")]
    fn route_to_approval(&mut self, selected: &[SelectedTableWork], cx: &mut Context<Self>) {
        let classification = selected
            .iter()
            .flat_map(|w| {
                w.changes
                    .iter()
                    .map(|c| c.risk)
                    .chain(w.table_action.as_ref().map(|(_, risk)| *risk))
            })
            .fold(ExecutionClassification::AdminSafe, |acc, risk| {
                acc.max(risk)
            });

        let payload = serde_json::json!({
            "profile_id": self.profile_id.to_string(),
            "database": self.database,
            "change_count": selected
                .iter()
                .map(|w| w.changes.len() + w.table_action.is_some() as usize)
                .sum::<usize>(),
        });
        let connection_id = self.profile_id.to_string();

        let enqueue = self.app_state.update(cx, |app, _| {
            app.request_mcp_execution(
                "user".to_string(),
                connection_id,
                "schema_diff.apply".to_string(),
                classification,
                payload,
            )
        });

        match enqueue {
            Ok(_) => {
                self.pending_toast = Some(PendingToast {
                    message: "Schema changes queued for approval.".to_string(),
                    is_error: false,
                });
            }
            Err(e) => {
                self.pending_toast = Some(PendingToast {
                    message: format!("Failed to queue for approval: {e}"),
                    is_error: true,
                });
            }
        }
        cx.notify();
    }

    #[cfg(not(feature = "mcp"))]
    fn route_to_approval(&mut self, _selected: &[SelectedTableWork], cx: &mut Context<Self>) {
        self.pending_toast = Some(PendingToast {
            message: "This connection requires approval, which is unavailable in this build."
                .to_string(),
            is_error: true,
        });
        cx.notify();
    }
}

/// Send-friendly resolution plan for one side of the diff.
enum SidePlan {
    Live {
        connection: Arc<dyn Connection>,
        database: Option<String>,
        shallow: Vec<TableInfo>,
    },
    Resolved(Vec<TableInfo>),
}

/// Back-fills full column/index detail for every shallow table via
/// `table_details`. A failure is propagated as an error rather than silently
/// degrading to the column-less shallow entry: a missing column set would make
/// the diff engine see every column as dropped, producing a wrong and
/// potentially destructive apply plan. Aborting the whole comparison is the
/// only safe response.
fn deep_resolve(
    connection: &dyn Connection,
    database: Option<&str>,
    shallow: &[TableInfo],
) -> Result<Vec<TableInfo>, String> {
    let db = database.unwrap_or_default();
    let mut resolved = Vec::with_capacity(shallow.len());

    for table in shallow {
        let details = connection
            .table_details(db, table.schema.as_deref(), &table.name)
            .map_err(|e| {
                format!(
                    "Failed to load column details for {}: {e}. The comparison was aborted to avoid a wrong diff.",
                    qualified(&TableRef {
                        schema: table.schema.clone(),
                        name: table.name.clone(),
                    })
                )
            })?;
        resolved.push(details);
    }

    Ok(resolved)
}

/// Turns raw `TableChange`s into render groups, partitioning modified tables via
/// the target driver's code generator and probing whole-table add/remove
/// through the driver's `generate_code` seam.
fn build_groups(
    connection: &dyn Connection,
    table_changes: Vec<dbflux_core::TableChange>,
) -> Vec<TableDiffGroup> {
    use dbflux_core::TableChange;

    let code_generator = connection.code_generator();
    let mut groups = Vec::new();

    for change in table_changes {
        match change {
            TableChange::TableAdded(info) => {
                let table = TableRef {
                    schema: info.schema.clone(),
                    name: info.name.clone(),
                };
                let action = TableLevelAction::Create(info);
                let probe = build_statements_for_table_action(connection, &action);
                let outcome = classify_table_action(action, probe);
                groups.push(TableDiffGroup {
                    header: qualified(&table),
                    table,
                    applicable: Vec::new(),
                    unsupported: Vec::new(),
                    table_action: Some(outcome),
                });
            }
            TableChange::TableRemoved(table) => {
                let action = TableLevelAction::Drop(table.clone());
                let probe = build_statements_for_table_action(connection, &action);
                let outcome = classify_table_action(action, probe);
                groups.push(TableDiffGroup {
                    header: qualified(&table),
                    table,
                    applicable: Vec::new(),
                    unsupported: Vec::new(),
                    table_action: Some(outcome),
                });
            }
            TableChange::TableModified { table, changes } => {
                let PartitionedChanges {
                    applicable,
                    unsupported,
                } = partition_table_changes(&table, &changes, code_generator);
                groups.push(TableDiffGroup {
                    header: qualified(&table),
                    table,
                    applicable,
                    unsupported,
                    table_action: None,
                });
            }
        }
    }

    groups.retain(|g| !g.is_empty());
    groups
}

/// Builds a `DdlApplyExecutor` for one table's selected work, attaching the
/// whole-table action when present.
fn build_executor_for_work(work: SelectedTableWork, deps: DdlApplyDeps) -> DdlApplyExecutor {
    let executor = DdlApplyExecutor::new(work.table, work.changes, deps);
    match work.table_action {
        Some((action, _risk)) => executor.with_table_action(action),
        None => executor,
    }
}

fn qualified(table: &TableRef) -> String {
    match &table.schema {
        Some(schema) => format!("{schema}.{}", table.name),
        None => table.name.clone(),
    }
}

fn describe_table_action(action: &TableLevelAction) -> String {
    match action {
        TableLevelAction::Create(info) => format!(
            "Create table {}",
            qualified(&TableRef {
                schema: info.schema.clone(),
                name: info.name.clone(),
            })
        ),
        TableLevelAction::Drop(table) => format!("Drop table {}", qualified(table)),
    }
}

/// Short human description of a single change for the diff row.
fn describe_change(change: &SchemaChange) -> String {
    match change {
        SchemaChange::ColumnAdded(c) => format!("Add column {} {}", c.name, c.type_name),
        SchemaChange::ColumnRemoved(c) => format!("Drop column {}", c.name),
        SchemaChange::ColumnTypeChanged { before, after } => {
            format!(
                "Change {} type {} → {}",
                before.name, before.type_name, after.type_name
            )
        }
        SchemaChange::NullabilityChanged { column, after, .. } => {
            if *after {
                format!("Make {column} nullable")
            } else {
                format!("Make {column} NOT NULL")
            }
        }
        SchemaChange::DefaultChanged { column, after, .. } => match after {
            Some(value) => format!("Set default on {column} to {value}"),
            None => format!("Drop default on {column}"),
        },
        SchemaChange::PrimaryKeyChanged { .. } => "Change primary key".to_string(),
        SchemaChange::ForeignKeyChanged => "Change foreign keys".to_string(),
        SchemaChange::IndexAdded(index) => format!("Add index {}", index.name),
        SchemaChange::IndexRemoved(index) => format!("Drop index {}", index.name),
    }
}

impl Focusable for SchemaDiffDocument {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<DocumentEvent> for SchemaDiffDocument {}

fn badge_variant(badge: RiskBadge) -> BadgeVariant {
    match badge {
        RiskBadge::Safe => BadgeVariant::Success,
        RiskBadge::Warning => BadgeVariant::Warning,
        RiskBadge::Destructive => BadgeVariant::Danger,
    }
}

impl SchemaDiffDocument {
    fn can_compute(&self) -> bool {
        match self.picker.mode {
            DiffMode::LiveVsLive => live_reference_ready(&self.reference),
            DiffMode::SnapshotVsLive => self.picker.selected_snapshot.is_some(),
        }
    }

    fn primary_button(
        &self,
        id: &'static str,
        label: &'static str,
        enabled: bool,
        cx: &mut Context<Self>,
        on_click: impl Fn(&mut Self, &mut Window, &mut Context<Self>) + 'static,
    ) -> impl IntoElement {
        let (primary, primary_foreground) = {
            let theme = cx.theme();
            (theme.primary, theme.primary_foreground)
        };
        div()
            .id(id)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .rounded(Radii::SM)
            .bg(primary)
            .text_size(FontSizes::SM)
            .when(enabled, |d| d.cursor_pointer().hover(|h| h.opacity(0.9)))
            .when(!enabled, |d| d.opacity(0.4))
            .child(Text::caption(label).color(primary_foreground))
            .when(enabled, move |d| {
                d.on_click(cx.listener(move |this, _, window, cx| on_click(this, window, cx)))
            })
    }

    fn secondary_button(
        &self,
        id: &'static str,
        label: &'static str,
        enabled: bool,
        cx: &mut Context<Self>,
        on_click: impl Fn(&mut Self, &mut Window, &mut Context<Self>) + 'static,
    ) -> impl IntoElement {
        let (secondary, muted) = {
            let theme = cx.theme();
            (theme.secondary, theme.muted)
        };
        div()
            .id(id)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .rounded(Radii::SM)
            .bg(secondary)
            .when(enabled, |d| d.cursor_pointer().hover(move |h| h.bg(muted)))
            .when(!enabled, |d| d.opacity(0.4))
            .child(Text::body(label).font_size(FontSizes::SM))
            .when(enabled, move |d| {
                d.on_click(cx.listener(move |this, _, window, cx| on_click(this, window, cx)))
            })
    }

    fn render_source_picker(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let border = cx.theme().border;
        let mode = self.picker.mode;

        let mode_toggle = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(self.mode_chip(
                "mode-live",
                "Live ↔ Live",
                mode == DiffMode::LiveVsLive,
                DiffMode::LiveVsLive,
                cx,
            ))
            .child(self.mode_chip(
                "mode-snapshot",
                "Snapshot ↔ Live",
                mode == DiffMode::SnapshotVsLive,
                DiffMode::SnapshotVsLive,
                cx,
            ));

        let reference = match mode {
            DiffMode::LiveVsLive => self.render_live_reference_list(cx).into_any_element(),
            DiffMode::SnapshotVsLive => self.render_snapshot_list(cx).into_any_element(),
        };

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .p(Spacing::MD)
            .border_b_1()
            .border_color(border)
            .child(Text::label_sm("Compare against").muted_foreground())
            .child(mode_toggle)
            .child(reference)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(self.primary_button(
                        "compute-diff",
                        "Compute Diff",
                        self.can_compute() && !self.is_busy(),
                        cx,
                        |this, _w, cx| this.compute_diff(cx),
                    )),
            )
    }

    fn mode_chip(
        &self,
        id: &'static str,
        label: &'static str,
        active: bool,
        mode: DiffMode,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let (primary, primary_foreground, secondary, muted) = {
            let theme = cx.theme();
            (
                theme.primary,
                theme.primary_foreground,
                theme.secondary,
                theme.muted,
            )
        };
        div()
            .id(id)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .rounded(Radii::SM)
            .cursor_pointer()
            .when(active, |d| d.bg(primary).text_color(primary_foreground))
            .when(!active, |d| d.bg(secondary).hover(move |h| h.bg(muted)))
            .child(Text::caption(label))
            .on_click(cx.listener(move |this, _, _, cx| this.set_mode(mode, cx)))
    }

    /// One selectable reference row (a database or a connection), styled the
    /// same way regardless of which group it belongs to.
    fn reference_option_row(
        &self,
        id: SharedString,
        label: String,
        selected: bool,
        cx: &mut Context<Self>,
        on_click: impl Fn(&mut Self, &mut Window, &mut Context<Self>) + 'static,
    ) -> AnyElement {
        let (primary, muted) = {
            let theme = cx.theme();
            (theme.primary, theme.muted)
        };
        div()
            .id(id)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .rounded(Radii::SM)
            .cursor_pointer()
            .when(selected, |d| d.bg(primary.opacity(0.15)))
            .hover(move |h| h.bg(muted))
            .child(Text::body(label))
            .on_click(cx.listener(move |this, _, window, cx| on_click(this, window, cx)))
            .into_any_element()
    }

    fn render_live_reference_list(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let database_candidates = same_connection_reference_databases(
            &self.connection_databases,
            self.database.as_deref(),
        );

        let connection_candidates: Vec<(Uuid, String)> = {
            let state = self.app_state.read(cx);
            state
                .connections()
                .iter()
                .filter(|(id, connected)| {
                    **id != self.profile_id
                        && connected.connection.metadata().category
                            == dbflux_core::DatabaseCategory::Relational
                })
                .map(|(id, connected)| (*id, connected.profile.name.clone()))
                .collect()
        };

        if database_candidates.is_empty() && connection_candidates.is_empty() {
            return div()
                .child(
                    Text::caption("Connect a second database or connection to compare against.")
                        .muted_foreground(),
                )
                .into_any_element();
        }

        let mut sections: Vec<AnyElement> = Vec::new();

        if !database_candidates.is_empty() {
            let mut rows: Vec<AnyElement> = vec![
                Text::label_sm("Other databases on this connection")
                    .muted_foreground()
                    .into_any_element(),
            ];
            for database in database_candidates {
                let selected = matches!(
                    &self.reference,
                    Some(ReferenceTarget::SameConnectionDatabase(chosen)) if chosen == &database
                );
                let database_for_click = database.clone();
                rows.push(self.reference_option_row(
                    SharedString::from(format!("ref-db-{database}")),
                    database,
                    selected,
                    cx,
                    move |this, _w, cx| {
                        this.select_same_connection_database(database_for_click.clone(), cx)
                    },
                ));
            }
            sections.push(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .children(rows)
                    .into_any_element(),
            );
        }

        if !connection_candidates.is_empty() {
            let mut rows: Vec<AnyElement> = vec![
                Text::label_sm("Other connections")
                    .muted_foreground()
                    .into_any_element(),
            ];
            for (id, name) in connection_candidates {
                let selected = matches!(
                    &self.reference,
                    Some(ReferenceTarget::OtherConnection { profile_id, .. }) if *profile_id == id
                );
                rows.push(self.reference_option_row(
                    SharedString::from(format!("ref-conn-{id}")),
                    name,
                    selected,
                    cx,
                    move |this, _w, cx| this.select_reference_connection(id, cx),
                ));
            }
            sections.push(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .children(rows)
                    .into_any_element(),
            );
        }

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .children(sections)
            .into_any_element()
    }

    fn render_snapshot_list(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let (primary, muted) = {
            let theme = cx.theme();
            (theme.primary, theme.muted)
        };

        if self.snapshots.is_empty() {
            return div()
                .child(
                    Text::caption("No snapshots captured for this connection yet.")
                        .muted_foreground(),
                )
                .into_any_element();
        }

        let mut rows: Vec<AnyElement> = Vec::new();
        for summary in &self.snapshots {
            let Ok(snapshot_id) = Uuid::parse_str(&summary.id) else {
                continue;
            };
            let selected = self.picker.selected_snapshot == Some(snapshot_id);
            let label = format!(
                "{}  ·  {:?}",
                format_captured_at(summary.captured_at),
                summary.depth
            );
            rows.push(
                div()
                    .id(SharedString::from(format!("snap-{}", summary.id)))
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .when(selected, |d| d.bg(primary.opacity(0.15)))
                    .hover(move |h| h.bg(muted))
                    .child(Text::body(label))
                    .on_click(
                        cx.listener(move |this, _, _, cx| this.select_snapshot(snapshot_id, cx)),
                    )
                    .into_any_element(),
            );
        }

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .children(rows)
            .into_any_element()
    }

    fn render_diff_list(&self, cx: &mut Context<Self>) -> AnyElement {
        let background = cx.theme().background;

        match &self.compute_state {
            ComputeState::Loading => {
                return diff_message_container()
                    .child(Text::body("Computing diff…").muted_foreground())
                    .into_any_element();
            }
            ComputeState::Idle => {
                return diff_message_container()
                    .child(
                        Text::body("Pick a source and run Compute Diff to see schema changes.")
                            .muted_foreground(),
                    )
                    .into_any_element();
            }
            ComputeState::Error(message) => {
                return diff_message_container()
                    .child(Text::body(message.clone()).danger())
                    .into_any_element();
            }
            ComputeState::Empty => {
                return diff_message_container()
                    .child(
                        Text::body("No differences found between the two schemas.")
                            .muted_foreground(),
                    )
                    .into_any_element();
            }
            ComputeState::Diff => {}
        }

        let mut groups: Vec<AnyElement> = Vec::with_capacity(self.groups.len());
        for index in 0..self.groups.len() {
            groups.push(self.render_group(index, cx));
        }

        div()
            .id("schema-diff-list-scroll")
            .flex_1()
            .track_scroll(&self.diff_scroll)
            .overflow_y_scroll()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .p(Spacing::MD)
            .bg(background)
            .children(groups)
            .into_any_element()
    }

    fn render_group(&self, group_index: usize, cx: &mut Context<Self>) -> AnyElement {
        let border = cx.theme().border;
        let group = &self.groups[group_index];

        let mut rows: Vec<AnyElement> = Vec::new();

        for (change_index, change) in group.applicable.iter().enumerate() {
            rows.push(self.render_change_row(group_index, change_index, change, cx));
        }

        for unsupported in &group.unsupported {
            rows.push(render_unsupported_row(unsupported));
        }

        if let Some(outcome) = &group.table_action {
            rows.push(self.render_table_action_row(group_index, outcome, cx));
        }

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .p(Spacing::SM)
            .rounded(Radii::MD)
            .border_1()
            .border_color(border)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(Icon::new(AppIcon::Table).size(Heights::ICON_SM).muted())
                    .child(Text::body(group.header.clone()).font_weight(FontWeight::MEDIUM)),
            )
            .children(rows)
            .into_any_element()
    }

    fn render_change_row(
        &self,
        group_index: usize,
        change_index: usize,
        change: &RiskedChange,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (border, primary, primary_foreground) = {
            let theme = cx.theme();
            (theme.border, theme.primary, theme.primary_foreground)
        };
        let checked = self.selected.contains(&(group_index, change_index));
        let badge = RiskBadge::from_classification(change.risk);
        let description = describe_change(&change.change);

        let checkbox = div()
            .id(SharedString::from(format!(
                "chk-{group_index}-{change_index}"
            )))
            .size(px(16.0)) // guardrail-allow: 16px checkbox box, no checkbox-size token
            .rounded(Radii::SM)
            .border_1()
            .border_color(border)
            .cursor_pointer()
            .when(checked, |d| d.bg(primary))
            .when(checked, |d| {
                d.child(
                    Icon::new(AppIcon::Check)
                        .size(px(12.0)) // guardrail-allow: 12px icon size, no ICON_XS token
                        .color(primary_foreground),
                )
            })
            .on_click(cx.listener(move |this, _, _, cx| {
                this.toggle_selection(group_index, change_index, cx)
            }));

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .py(Spacing::XS)
            .child(checkbox)
            .child(Badge::new(badge.label(), badge_variant(badge)))
            .child(Text::body(description))
            .into_any_element()
    }

    fn render_table_action_row(
        &self,
        group_index: usize,
        outcome: &TableActionOutcome,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match outcome {
            TableActionOutcome::Applicable { action, risk } => {
                let (border, primary, primary_foreground) = {
                    let theme = cx.theme();
                    (theme.border, theme.primary, theme.primary_foreground)
                };
                let checked = self.selected_table_actions.contains(&group_index);
                let badge = RiskBadge::from_classification(*risk);
                let description = describe_table_action(action);

                let checkbox = div()
                    .id(SharedString::from(format!("chk-table-{group_index}")))
                    .size(px(16.0)) // guardrail-allow: 16px checkbox box, no checkbox-size token
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(border)
                    .cursor_pointer()
                    .when(checked, |d| d.bg(primary))
                    .when(checked, |d| {
                        d.child(
                            Icon::new(AppIcon::Check)
                                .size(px(12.0)) // guardrail-allow: 12px icon size, no ICON_XS token
                                .color(primary_foreground),
                        )
                    })
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.toggle_table_action_selection(group_index, cx)
                    }));

                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .py(Spacing::XS)
                    .child(checkbox)
                    .child(Badge::new(badge.label(), badge_variant(badge)))
                    .child(Text::body(description))
                    .into_any_element()
            }
            TableActionOutcome::Unsupported {
                is_create,
                reason,
                followup,
                ..
            } => {
                let description = if *is_create {
                    "Create table"
                } else {
                    "Drop table"
                };
                let mut reason_text = reason.clone();
                if let Some(followup) = followup {
                    reason_text = format!("{reason_text} (see {followup})");
                }

                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .py(Spacing::XS)
                    .child(Badge::new("Unsupported", BadgeVariant::Neutral))
                    .child(Text::body(description))
                    .child(Text::caption(reason_text).muted_foreground())
                    .into_any_element()
            }
        }
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let border = cx.theme().border;
        let has_selection = !self.selected.is_empty() || !self.selected_table_actions.is_empty();

        div()
            .flex()
            .items_center()
            .justify_end()
            .gap(Spacing::SM)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .border_t_1()
            .border_color(border)
            .child(self.secondary_button(
                "preview-ddl",
                "Preview DDL",
                has_selection && !self.is_busy(),
                cx,
                |this, _w, cx| this.open_preview(cx),
            ))
            .child(self.primary_button(
                "apply-ddl",
                "Apply…",
                has_selection && !self.is_busy(),
                cx,
                |this, _w, cx| this.request_apply(cx),
            ))
    }
}

fn diff_message_container() -> Div {
    div()
        .flex_1()
        .flex()
        .flex_col()
        .items_center()
        .justify_center()
        .p(Spacing::LG)
}

fn render_unsupported_row(unsupported: &UnsupportedChange) -> AnyElement {
    let mut reason = unsupported.reason.clone();
    if let Some(followup) = &unsupported.followup {
        reason = format!("{reason} (see {followup})");
    }

    div()
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .py(Spacing::XS)
        .child(Badge::new("Unsupported", BadgeVariant::Neutral))
        .child(Text::body(describe_change(&unsupported.change)))
        .child(Text::caption(reason).muted_foreground())
        .into_any_element()
}

fn format_captured_at(millis: i64) -> String {
    format!(
        "captured {}",
        format_timestamp_ms(millis, TimestampDisplayMode::Local)
    )
}

impl Render for SchemaDiffDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(ddl) = self.pending_preview.take() {
            self.sql_preview_modal.update(cx, |modal, cx| {
                modal.open_query_preview(QueryLanguage::Sql, "DDL", ddl, window, cx);
            });
        }

        if let Some(request) = self.pending_confirm.take() {
            self.confirm_modal.update(cx, |modal, cx| {
                modal.open(request, window, cx);
            });
        }

        if std::mem::take(&mut self.pending_apply) {
            self.run_apply(cx);
        }

        flush_pending_toast(self.pending_toast.take(), window, cx);

        let theme = cx.theme().clone();
        let focus_handle = self.focus_handle.clone();

        div()
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .bg(theme.background)
            .track_focus(&focus_handle)
            .child(self.render_source_picker(cx))
            .child(self.render_diff_list(cx))
            .child(self.render_footer(cx))
            .child(self.sql_preview_modal.clone())
            .child(self.confirm_modal.clone())
    }
}

#[cfg(test)]
mod tests {
    // Import only what the tests need — deliberately NOT `use super::*`, which
    // would re-glob `gpui::*` into this module and trigger pathological
    // `#[test]` macro-expansion recursion in this GPUI-heavy crate.
    use super::{ComputeState, deep_resolve, document_state_for};
    use crate::types::DocumentState;
    use dbflux_core::{
        CodeGenerator, ColumnInfo, Connection, DatabaseCategory, DbError, DbKind,
        DefaultSqlDialect, DriverCapabilities, DriverMetadata, DriverMetadataBuilder,
        NoOpCodeGenerator, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
        SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo,
    };

    // ── FIX-2: identical-schema comparison is Empty (Clean), not Error ──────

    #[test]
    fn empty_compute_state_is_clean_not_error() {
        assert_eq!(
            document_state_for(&ComputeState::Empty),
            DocumentState::Clean,
            "a clean no-diff result must not render as an error"
        );
    }

    #[test]
    fn error_and_loading_states_map_distinctly() {
        assert_eq!(
            document_state_for(&ComputeState::Error("boom".to_string())),
            DocumentState::Error
        );
        assert_eq!(
            document_state_for(&ComputeState::Loading),
            DocumentState::Loading
        );
        assert_eq!(
            document_state_for(&ComputeState::Idle),
            DocumentState::Clean
        );
        assert_eq!(
            document_state_for(&ComputeState::Diff),
            DocumentState::Clean
        );
    }

    // ── FIX-3: deep_resolve propagates table_details failures ───────────────

    struct DeepResolveFake {
        meta: DriverMetadata,
        dialect: DefaultSqlDialect,
        codegen: NoOpCodeGenerator,
        fail_table_details: bool,
    }

    impl DeepResolveFake {
        fn new(fail_table_details: bool) -> Self {
            let meta = DriverMetadataBuilder::new(
                "test",
                "Test",
                DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .capabilities(DriverCapabilities::empty())
            .build();
            Self {
                meta,
                dialect: DefaultSqlDialect,
                codegen: NoOpCodeGenerator,
                fail_table_details,
            }
        }
    }

    impl Connection for DeepResolveFake {
        fn metadata(&self) -> &DriverMetadata {
            &self.meta
        }
        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }
        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            Ok(QueryResult::empty())
        }
        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }
        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Err(DbError::NotSupported("stub".to_string()))
        }
        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }
        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }
        fn dialect(&self) -> &dyn SqlDialect {
            &self.dialect
        }
        fn code_generator(&self) -> &dyn CodeGenerator {
            &self.codegen
        }
        fn table_details(
            &self,
            _database: &str,
            schema: Option<&str>,
            table: &str,
        ) -> Result<TableInfo, DbError> {
            if self.fail_table_details {
                return Err(DbError::NotSupported("cannot introspect".to_string()));
            }
            Ok(TableInfo {
                name: table.to_string(),
                schema: schema.map(str::to_string),
                columns: Some(vec![ColumnInfo {
                    name: "id".to_string(),
                    type_name: "integer".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    enum_values: None,
                }]),
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: Default::default(),
                child_items: None,
            })
        }
    }

    fn shallow_table(name: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some("public".to_string()),
            // Deliberately column-less: this is the shallow entry the old code
            // silently degraded to on failure.
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
        }
    }

    #[test]
    fn deep_resolve_propagates_table_details_error() {
        let connection = DeepResolveFake::new(true);
        let shallow = vec![shallow_table("users")];

        let result = deep_resolve(&connection, Some("app"), &shallow);

        assert!(
            result.is_err(),
            "a failed table_details must abort the comparison, not degrade to the shallow entry"
        );
        assert!(result.unwrap_err().contains("users"));
    }

    #[test]
    fn deep_resolve_backfills_columns_on_success() {
        let connection = DeepResolveFake::new(false);
        let shallow = vec![shallow_table("users")];

        let resolved = deep_resolve(&connection, Some("app"), &shallow)
            .expect("resolution should succeed when table_details succeeds");

        assert_eq!(resolved.len(), 1);
        assert!(
            resolved[0].columns.is_some(),
            "the resolved table must carry the fetched columns, not the column-less shallow entry"
        );
    }
}
