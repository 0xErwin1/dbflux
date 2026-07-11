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

use super::apply::{DdlApplyDeps, DdlApplyExecutor, DdlApplyOutcome};
use super::diff_source::{
    DiffMode, PartitionedChanges, RiskBadge, SourcePicker, UnsupportedChange,
    partition_table_changes,
};
use crate::handle::DocumentEvent;
use crate::types::{DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot, DocumentState};

/// One table's slice of the diff, grouped for rendering.
struct TableDiffGroup {
    table: TableRef,
    /// Human header, e.g. "public.users".
    header: String,
    /// Changes the driver can apply, with a stable index used for selection.
    appliable: Vec<RiskedChange>,
    /// Changes surfaced explicitly as unsupported (never applied).
    unsupported: Vec<UnsupportedChange>,
    /// Present for whole-table add/remove, which this executor does not apply.
    table_level_note: Option<String>,
}

impl TableDiffGroup {
    fn is_empty(&self) -> bool {
        self.appliable.is_empty() && self.unsupported.is_empty() && self.table_level_note.is_none()
    }
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
    /// Second live connection chosen as the reference in `LiveVsLive` mode.
    reference_profile: Option<Uuid>,
    groups: Vec<TableDiffGroup>,
    /// Selected appliable changes as `(group_index, appliable_index)`.
    selected: HashSet<(usize, usize)>,
    /// Snapshot summaries for the target profile/database, loaded when the
    /// snapshot-to-live mode is selected.
    snapshots: Vec<dbflux_storage::repositories::sch_schema_snapshots::SchemaSnapshotSummary>,

    is_loading: bool,
    has_computed: bool,
    status_message: Option<String>,
    pending_toast: Option<PendingToast>,

    sql_preview_modal: Entity<SqlPreviewModal>,
    confirm_modal: Entity<ModalMutationConfirmHard>,

    pending_preview: Option<String>,
    pending_confirm: Option<MutationConfirmHardRequest>,
    pending_apply: bool,

    focus_handle: FocusHandle,
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

        Self {
            id: DocumentId::new(),
            app_state,
            profile_id,
            database,
            title,
            picker: SourcePicker::default(),
            reference_profile: None,
            groups: Vec::new(),
            selected: HashSet::new(),
            snapshots: Vec::new(),
            is_loading: false,
            has_computed: false,
            status_message: None,
            pending_toast: None,
            sql_preview_modal,
            confirm_modal,
            pending_preview: None,
            pending_confirm: None,
            pending_apply: false,
            focus_handle: cx.focus_handle(),
            _subscriptions: vec![confirm_sub],
        }
    }

    // ── Document API (mirrored by the pane) ───────────────────────────────

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn state(&self) -> DocumentState {
        if self.is_loading {
            DocumentState::Loading
        } else if self.status_message.is_some() && self.groups.is_empty() && self.has_computed {
            DocumentState::Error
        } else {
            DocumentState::Clean
        }
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
        self.has_computed = false;
        self.groups.clear();
        self.selected.clear();
        if mode == DiffMode::SnapshotVsLive {
            self.load_snapshots(cx);
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
        self.has_computed = false;
        self.groups.clear();
        self.selected.clear();
        cx.notify();
    }

    fn select_reference_profile(&mut self, other_profile_id: Uuid, cx: &mut Context<Self>) {
        self.picker.mode = DiffMode::LiveVsLive;
        // Reuse selected_snapshot? No — store reference profile separately.
        self.reference_profile = Some(other_profile_id);
        self.has_computed = false;
        self.groups.clear();
        self.selected.clear();
        cx.notify();
    }

    // ── Diff computation ──────────────────────────────────────────────────

    fn compute_diff(&mut self, cx: &mut Context<Self>) {
        let state = self.app_state.read(cx);

        let Some(target) = state.connections().get(&self.profile_id) else {
            self.status_message = Some("Target connection is no longer available.".to_string());
            self.has_computed = true;
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
            DiffMode::LiveVsLive => {
                let Some(other_id) = self.reference_profile else {
                    self.status_message =
                        Some("Pick a second live connection to compare against.".to_string());
                    self.has_computed = true;
                    cx.notify();
                    return;
                };
                let Some(other) = state.connections().get(&other_id) else {
                    self.status_message =
                        Some("The chosen reference connection is not connected.".to_string());
                    self.has_computed = true;
                    cx.notify();
                    return;
                };
                let other_db = other.active_database.clone();
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
            DiffMode::SnapshotVsLive => {
                let Some(snapshot_id) = self.picker.selected_snapshot else {
                    self.status_message = Some("Pick a snapshot to compare against.".to_string());
                    self.has_computed = true;
                    cx.notify();
                    return;
                };
                match state.schema_snapshots.get(&snapshot_id.to_string()) {
                    Ok(Some(record)) => SidePlan::Resolved(record.tables),
                    Ok(None) => {
                        self.status_message =
                            Some("Selected snapshot no longer exists.".to_string());
                        self.has_computed = true;
                        cx.notify();
                        return;
                    }
                    Err(e) => {
                        self.status_message = Some(format!("Failed to load snapshot: {e}"));
                        self.has_computed = true;
                        cx.notify();
                        return;
                    }
                }
            }
        };

        self.is_loading = true;
        self.status_message = None;
        cx.notify();

        let target_db_for_task = target_db.clone();

        let task = cx.background_executor().spawn(async move {
            // `before` = target live (DDL applies here); `after` = reference.
            let before = deep_resolve(
                &*target_connection,
                target_db_for_task.as_deref(),
                &target_shallow,
            );
            let after = match reference_plan {
                SidePlan::Live {
                    connection,
                    database,
                    shallow,
                } => deep_resolve(&*connection, database.as_deref(), &shallow),
                SidePlan::Resolved(tables) => tables,
            };

            let table_changes = diff_schema(&before, &after);
            build_groups(&*target_connection, table_changes)
        });

        cx.spawn(async move |this, cx| {
            let groups = task.await;
            cx.update(|cx| {
                this.update(cx, |doc, cx| {
                    doc.is_loading = false;
                    doc.has_computed = true;
                    doc.groups = groups;
                    doc.selected = doc.default_selection();
                    if doc.groups.is_empty() {
                        doc.status_message =
                            Some("No differences found between the two schemas.".to_string());
                    }
                    cx.notify();
                })
            })
            .ok();
        })
        .detach();
    }

    /// Default selection = every appliable change checked.
    fn default_selection(&self) -> HashSet<(usize, usize)> {
        let mut set = HashSet::new();
        for (group_index, group) in self.groups.iter().enumerate() {
            for change_index in 0..group.appliable.len() {
                set.insert((group_index, change_index));
            }
        }
        set
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

    /// Collects the selected appliable changes grouped by table.
    fn selected_changes_by_table(&self) -> Vec<(TableRef, Vec<RiskedChange>)> {
        let mut out: Vec<(TableRef, Vec<RiskedChange>)> = Vec::new();

        for (group_index, group) in self.groups.iter().enumerate() {
            let mut picked = Vec::new();
            for (change_index, change) in group.appliable.iter().enumerate() {
                if self.selected.contains(&(group_index, change_index)) {
                    picked.push(change.clone());
                }
            }
            if !picked.is_empty() {
                out.push((group.table.clone(), picked));
            }
        }

        out
    }

    fn has_destructive_selection(&self) -> bool {
        for (group_index, group) in self.groups.iter().enumerate() {
            for (change_index, change) in group.appliable.iter().enumerate() {
                if self.selected.contains(&(group_index, change_index))
                    && RiskBadge::from_classification(change.risk) == RiskBadge::Destructive
                {
                    return true;
                }
            }
        }
        false
    }

    // ── Preview ───────────────────────────────────────────────────────────

    fn open_preview(&mut self, cx: &mut Context<Self>) {
        let selected = self.selected_changes_by_table();
        if selected.is_empty() {
            self.pending_toast = Some(PendingToast {
                message: "Select at least one change to preview.".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        let Some(connection) = self.app_state.read(cx).get_connection(self.profile_id) else {
            self.pending_toast = Some(PendingToast {
                message: "Target connection is no longer available.".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        };

        let mut statements: Vec<String> = Vec::new();
        for (table, changes) in selected {
            let executor = DdlApplyExecutor::new(
                table,
                changes,
                DdlApplyDeps {
                    connection: Arc::clone(&connection),
                    event_sink: None,
                    policy: MutationPolicy::Allowed,
                },
            );
            match executor.preview_statements() {
                Ok(stmts) => statements.extend(stmts),
                Err(e) => {
                    self.pending_toast = Some(PendingToast {
                        message: format!("Cannot build preview: {e}"),
                        is_error: true,
                    });
                    cx.notify();
                    return;
                }
            }
        }

        self.pending_preview = Some(statements.join(";\n\n") + ";");
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

        let total: usize = selected.iter().map(|(_, c)| c.len()).sum();
        let summary = format!(
            "Apply {total} schema change(s) to {}",
            self.database.as_deref().unwrap_or("this connection")
        );

        // Build a read-only DDL preview string for the confirm body.
        let sql_preview = self
            .app_state
            .read(cx)
            .get_connection(self.profile_id)
            .map(|connection| {
                let mut statements = Vec::new();
                for (table, changes) in &selected {
                    let executor = DdlApplyExecutor::new(
                        table.clone(),
                        changes.clone(),
                        DdlApplyDeps {
                            connection: Arc::clone(&connection),
                            event_sink: None,
                            policy: MutationPolicy::Allowed,
                        },
                    );
                    if let Ok(stmts) = executor.preview_statements() {
                        statements.extend(stmts);
                    }
                }
                statements.join(";\n\n") + ";"
            })
            .unwrap_or_default();

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

        self.is_loading = true;
        cx.notify();

        let cancel = crate::task_runner::MutationCancelHandle::new();

        let task = cx.background_executor().spawn(async move {
            let mut applied = 0usize;
            for (table, changes) in selected {
                let executor = DdlApplyExecutor::new(
                    table,
                    changes,
                    DdlApplyDeps {
                        connection: Arc::clone(&connection),
                        event_sink: event_sink.clone(),
                        policy,
                    },
                );
                match executor.apply(&cancel) {
                    Ok(DdlApplyOutcome::Success {
                        statements_executed,
                        ..
                    }) => applied += statements_executed,
                    Ok(other) => {
                        return Err(format!("Apply stopped: {other:?}"));
                    }
                    Err(e) => return Err(e.to_string()),
                }
            }
            Ok(applied)
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;
            cx.update(|cx| {
                this.update(cx, |doc, cx| {
                    doc.is_loading = false;
                    match result {
                        Ok(applied) => {
                            doc.pending_toast = Some(PendingToast {
                                message: format!("Applied {applied} DDL statement(s)."),
                                is_error: false,
                            });
                            // Re-run the diff so the list reflects the new state.
                            doc.has_computed = false;
                            doc.groups.clear();
                            doc.selected.clear();
                        }
                        Err(message) => {
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
    fn route_to_approval(
        &mut self,
        selected: &[(TableRef, Vec<RiskedChange>)],
        cx: &mut Context<Self>,
    ) {
        let classification = selected
            .iter()
            .flat_map(|(_, changes)| changes.iter())
            .map(|c| c.risk)
            .fold(ExecutionClassification::AdminSafe, |acc, risk| {
                acc.max(risk)
            });

        let payload = serde_json::json!({
            "profile_id": self.profile_id.to_string(),
            "database": self.database,
            "change_count": selected.iter().map(|(_, c)| c.len()).sum::<usize>(),
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
    fn route_to_approval(
        &mut self,
        _selected: &[(TableRef, Vec<RiskedChange>)],
        cx: &mut Context<Self>,
    ) {
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
/// `table_details`, keeping the shallow entry on a per-table failure.
fn deep_resolve(
    connection: &dyn Connection,
    database: Option<&str>,
    shallow: &[TableInfo],
) -> Vec<TableInfo> {
    let db = database.unwrap_or_default();
    shallow
        .iter()
        .map(|table| {
            connection
                .table_details(db, table.schema.as_deref(), &table.name)
                .unwrap_or_else(|_| table.clone())
        })
        .collect()
}

/// Turns raw `TableChange`s into render groups, partitioning modified tables via
/// the target driver's code generator.
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
                groups.push(TableDiffGroup {
                    header: qualified(&table),
                    table,
                    appliable: Vec::new(),
                    unsupported: Vec::new(),
                    table_level_note: Some(
                        "New table — creating whole tables is not applied here.".to_string(),
                    ),
                });
            }
            TableChange::TableRemoved(table) => {
                groups.push(TableDiffGroup {
                    header: qualified(&table),
                    table,
                    appliable: Vec::new(),
                    unsupported: Vec::new(),
                    table_level_note: Some(
                        "Removed table — dropping whole tables is not applied here.".to_string(),
                    ),
                });
            }
            TableChange::TableModified { table, changes } => {
                let PartitionedChanges {
                    appliable,
                    unsupported,
                } = partition_table_changes(&table, &changes, code_generator);
                groups.push(TableDiffGroup {
                    header: qualified(&table),
                    table,
                    appliable,
                    unsupported,
                    table_level_note: None,
                });
            }
        }
    }

    groups.retain(|g| !g.is_empty());
    groups
}

fn qualified(table: &TableRef) -> String {
    match &table.schema {
        Some(schema) => format!("{schema}.{}", table.name),
        None => table.name.clone(),
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
            DiffMode::LiveVsLive => self.reference_profile.is_some(),
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
                        self.can_compute() && !self.is_loading,
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

    fn render_live_reference_list(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let (primary, muted) = {
            let theme = cx.theme();
            (theme.primary, theme.muted)
        };

        let candidates: Vec<(Uuid, String)> = {
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

        let mut rows: Vec<AnyElement> = Vec::new();
        for (id, name) in candidates {
            let selected = self.reference_profile == Some(id);
            rows.push(
                div()
                    .id(SharedString::from(format!("ref-{id}")))
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .when(selected, |d| d.bg(primary.opacity(0.15)))
                    .hover(move |h| h.bg(muted))
                    .child(Text::body(name))
                    .on_click(
                        cx.listener(move |this, _, _, cx| this.select_reference_profile(id, cx)),
                    )
                    .into_any_element(),
            );
        }

        if rows.is_empty() {
            return div()
                .child(
                    Text::caption("No other relational connections are open.").muted_foreground(),
                )
                .into_any_element();
        }

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .children(rows)
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
                format_millis(summary.captured_at),
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

        if self.is_loading {
            return div()
                .p(Spacing::LG)
                .child(Text::body("Computing diff…").muted_foreground())
                .into_any_element();
        }

        if !self.has_computed {
            return div()
                .p(Spacing::LG)
                .child(
                    Text::body("Pick a source and run Compute Diff to see schema changes.")
                        .muted_foreground(),
                )
                .into_any_element();
        }

        if self.groups.is_empty() {
            let message = self
                .status_message
                .clone()
                .unwrap_or_else(|| "No differences found.".to_string());
            return div()
                .p(Spacing::LG)
                .child(Text::body(message).muted_foreground())
                .into_any_element();
        }

        let mut groups: Vec<AnyElement> = Vec::with_capacity(self.groups.len());
        for index in 0..self.groups.len() {
            groups.push(self.render_group(index, cx));
        }

        div()
            .flex_1()
            .overflow_hidden()
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

        for (change_index, change) in group.appliable.iter().enumerate() {
            rows.push(self.render_change_row(group_index, change_index, change, cx));
        }

        for unsupported in &group.unsupported {
            rows.push(render_unsupported_row(unsupported));
        }

        if let Some(note) = &group.table_level_note {
            rows.push(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .py(Spacing::XS)
                    .child(Badge::new("Unsupported", BadgeVariant::Neutral))
                    .child(Text::caption(note.clone()).muted_foreground())
                    .into_any_element(),
            );
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

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let border = cx.theme().border;
        let has_selection = !self.selected.is_empty();

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
                has_selection && !self.is_loading,
                cx,
                |this, _w, cx| this.open_preview(cx),
            ))
            .child(self.primary_button(
                "apply-ddl",
                "Apply…",
                has_selection && !self.is_loading,
                cx,
                |this, _w, cx| this.request_apply(cx),
            ))
    }
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

fn format_millis(millis: i64) -> String {
    let secs = (millis.max(0) as u64) / 1000;
    format!("captured @ {secs}")
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
