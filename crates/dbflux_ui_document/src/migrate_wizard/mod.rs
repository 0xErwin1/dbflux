//! Migrate wizard: a five-phase flow (Source & Target → Tables Mapping →
//! Options → Confirm → Run) rendered inside a large, vertically centered
//! modal with a left phase rail. Each phase is a self-contained child
//! entity; this module owns the [`WizardPhase`] state machine that mounts
//! them, resolves the source/target connections and metadata between phases
//! (via the shared `prepare_fetch_*` seam), pre-computes the FK load order on
//! the `Options` → `Confirm` transition, and drives forward/back navigation.
//!
//! Reached from the sidebar's multi-select "Migrate…" action, which
//! pre-populates `source_profile_id` / `source_database` / `source_tables`.
//! Engine contracts are preserved verbatim: `TableMigrationConfig::to_overrides()`,
//! the `open(profile_id, database, tables, …)` signature, and `run_migration`
//! semantics (the run itself lives in [`confirm_run`]).

mod column_mapping;
pub mod confirm_run;
pub mod mapping;
pub mod options;
pub mod phases;
pub mod source_target;
pub mod tree_model;

use std::sync::Arc;

use dbflux_components::composites::{RailItem, render_wizard_rail};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::{
    ColumnInfo, Connection, DbError, DriverCapabilities, LogErr, SchemaCacheKey,
    SchemaForeignKeyInfo, TableInfo, TableRef, TransferColumn, topological_order,
};
use dbflux_transfer::TableTransferStatus;
use dbflux_transfer::migration::{MigratedTable, MigrationOptions, MigrationTablePlan};
use dbflux_ui_base::app_state_entity::AppStateEntity;
use dbflux_ui_base::modal_frame::ModalFrame;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

pub use column_mapping::TableMigrationConfig;
use confirm_run::{ConfirmRunEvent, ConfirmRunInputs, ConfirmRunPhase, decide_order};
use mapping::{MappingChanged, MappingPhase};
use options::{OptionsChanged, OptionsPhase};
use phases::{
    MappingRowPlan, RAIL_PHASES, RailEntry, RunState, WizardPhase, rail_entries,
    tables_mapping_confirm_warnings,
};
use source_target::{SourceTargetChanged, SourceTargetPhase};

/// Whether an `Err(String)` returned by one of the shared
/// `AppStateEntity::prepare_fetch_*` seams means "the data is already cached"
/// rather than a real failure — mirrors the sidebar's `spawn_fetch_*`
/// handling (`crates/dbflux_ui_sidebar/src/table_loading.rs`), where the same
/// sentinel strings gate whether to report an error or simply proceed with
/// what is already cached.
fn is_already_cached_sentinel(error: &str) -> bool {
    matches!(
        error,
        "Table details already cached" | "Schema foreign keys already cached"
    )
}

/// Converts driver-reported column metadata into the transfer engine's
/// column shape — identical to the wizard's original inline conversion.
fn to_transfer_columns(columns: Vec<ColumnInfo>) -> Vec<TransferColumn> {
    columns
        .into_iter()
        .map(|c| TransferColumn {
            name: c.name,
            type_name: Some(c.type_name),
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
        })
        .collect()
}

/// Assembles the engine's per-table migration plans from the wizard's
/// adjustable [`TableMigrationConfig`] rows — the same field mapping the
/// wizard previously built inline in `start_migration`, extracted here so
/// it is unit-testable without a live wizard entity or GPUI context.
fn build_migration_table_plans<'a>(
    configs: impl IntoIterator<Item = &'a TableMigrationConfig>,
) -> Vec<MigrationTablePlan> {
    configs
        .into_iter()
        .map(|config| MigrationTablePlan {
            source_table: config.source_table.clone(),
            source_columns: config.source_columns.clone(),
            target_schema: config.target_schema.clone(),
            target_table: config.target_table.clone(),
            mapping_mode: config.mapping_mode,
            column_overrides: Some(config.to_overrides()),
            estimated_total: None,
        })
        .collect()
}

/// Assembles [`MigrationOptions`] from the wizard's resolved run settings.
/// `target_database` is taken as an explicit parameter (rather than
/// re-derived from a live connection here) so a future target-container
/// picker can feed it directly without this function changing shape.
#[allow(clippy::too_many_arguments)]
fn build_migration_options(
    segment_size: u32,
    source_database: String,
    target_database: String,
    destructive_confirmed: bool,
    disable_referential_integrity: bool,
    manual_order: Option<Vec<TableRef>>,
) -> MigrationOptions {
    MigrationOptions {
        segment_size,
        source_database,
        target_database,
        destructive_confirmed,
        disable_referential_integrity,
        manual_order,
    }
}

/// Maps the wizard's [`RailEntry`]s to the shared rail composite's
/// domain-free [`RailItem`]s, in the same fixed `RAIL_PHASES` order the
/// caller uses to translate a clicked index back into a [`WizardPhase`].
fn to_rail_items(current: WizardPhase) -> Vec<RailItem> {
    rail_entries(current)
        .into_iter()
        .map(|entry: RailEntry| RailItem {
            label: entry.phase.label().into(),
            completed: entry.completed,
            current: entry.current,
        })
        .collect()
}

/// Outcome of fetching one table's details through the shared
/// `prepare_fetch_table_details` seam. `NotFound` maps only a genuine
/// driver-reported "object does not exist" (`DbError::ObjectNotFound`) — the
/// expected "target table will be created" signal, see
/// [`TableMigrationConfig::new`]'s `target_exists` flag. Any other
/// execute-time failure is returned as a real error so a transient fetch
/// failure is never silently classified as "will be created".
enum TableDetailsFetch {
    Found(Box<TableInfo>),
    NotFound(String),
}

/// Reads (or fetches, on the background executor) one table's details
/// through the shared `AppStateEntity::prepare_fetch_table_details` seam,
/// treating the "already cached" sentinel as success by reading the already
/// populated `ConnectedProfile` cache instead of re-fetching — mirrors the
/// sidebar's `spawn_fetch_table_details` (Reuse Audit: replaces the wizard's
/// former bespoke `Connection::table_details` closure).
async fn fetch_table_details_via_seam(
    app_state: &Entity<AppStateEntity>,
    profile_id: Uuid,
    database: &str,
    table_ref: &TableRef,
    cx: &mut AsyncApp,
) -> Result<TableDetailsFetch, String> {
    let prepared = cx
        .update(|cx| {
            app_state.read(cx).prepare_fetch_table_details(
                profile_id,
                database,
                table_ref.schema.as_deref(),
                &table_ref.name,
            )
        })
        .map_err(|e| e.to_string())?;

    let params = match prepared {
        Ok(params) => params,
        Err(e) if is_already_cached_sentinel(&e) => {
            let cached = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .get_table_details(
                            profile_id,
                            database,
                            table_ref.schema.as_deref(),
                            &table_ref.name,
                        )
                        .cloned()
                })
                .map_err(|e| e.to_string())?;
            return Ok(match cached {
                Some(info) => TableDetailsFetch::Found(Box::new(info)),
                None => TableDetailsFetch::NotFound(
                    "Table details reported as cached but the cache was empty".to_string(),
                ),
            });
        }
        Err(e) => return Err(e),
    };

    let execute_result = cx
        .background_executor()
        .spawn(async move { params.execute() })
        .await;

    match execute_result {
        Ok(result) => {
            let details = result.details.clone();
            cx.update(|cx| {
                app_state.update(cx, |state, _| {
                    state.set_table_details(
                        result.profile_id,
                        result.database.clone(),
                        result.schema.clone(),
                        result.table.clone(),
                        result.details,
                    );
                    state.set_dependents(
                        result.profile_id,
                        result.database,
                        result.schema,
                        result.table,
                        result.dependents,
                    );
                });
            })
            .map_err(|e| e.to_string())?;
            Ok(TableDetailsFetch::Found(Box::new(details)))
        }
        Err(error @ DbError::ObjectNotFound(_)) => {
            Ok(TableDetailsFetch::NotFound(error.to_string()))
        }
        Err(error) => Err(error.to_string()),
    }
}

/// Reads (or fetches, on the background executor) one schema's foreign keys
/// through the shared `AppStateEntity::prepare_fetch_schema_foreign_keys`
/// seam, treating the "already cached" sentinel as success by reading the
/// already populated `ConnectedProfile` cache — mirrors
/// [`fetch_table_details_via_seam`]. Replaces the wizard's former
/// synchronous foreground `Connection::schema_foreign_keys` call.
async fn fetch_schema_foreign_keys_via_seam(
    app_state: &Entity<AppStateEntity>,
    profile_id: Uuid,
    database: &str,
    schema: Option<&str>,
    cx: &mut AsyncApp,
) -> Result<Vec<SchemaForeignKeyInfo>, String> {
    let prepared = cx
        .update(|cx| {
            app_state
                .read(cx)
                .prepare_fetch_schema_foreign_keys(profile_id, database, schema)
        })
        .map_err(|e| e.to_string())?;

    let params = match prepared {
        Ok(params) => params,
        Err(e) if is_already_cached_sentinel(&e) => {
            let key = SchemaCacheKey::new(database, schema.map(str::to_string));
            return cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .and_then(|connected| connected.schema_foreign_keys.get(&key))
                        .cloned()
                        .unwrap_or_default()
                })
                .map_err(|e| e.to_string());
        }
        Err(e) => return Err(e),
    };

    let execute_result = cx
        .background_executor()
        .spawn(async move { params.execute() })
        .await?;

    let foreign_keys = execute_result.foreign_keys.clone();
    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.set_schema_foreign_keys(
                execute_result.profile_id,
                execute_result.database,
                execute_result.schema,
                execute_result.foreign_keys,
            );
        });
    })
    .map_err(|e| e.to_string())?;

    Ok(foreign_keys)
}

/// Whether a successfully fetched target `table_details` proves the target
/// relation already exists. An existing relation always projects at least one
/// column, so a `None`/empty column set is treated as "does not exist" — some
/// drivers (Postgres/MySQL) return `Ok(TableInfo)` with no columns for a
/// missing relation instead of `DbError::ObjectNotFound`, and that Ok-but-empty
/// result must classify the row as `Create` (mirror the source columns), never
/// `Existing`.
fn target_columns_prove_existing(columns: Option<&[ColumnInfo]>) -> bool {
    columns.is_some_and(|columns| !columns.is_empty())
}

/// Builds one [`TableMigrationConfig`] per checked source table by fetching
/// the source and target table schemas through the shared metadata seam: the
/// source columns must exist (a failure here is fatal), while a missing target
/// table is the expected "will be created" signal rather than an error. The
/// same field mapping the pre-redesign wizard produced inline, extracted so
/// the `Source & Target` → `Tables Mapping` transition stays readable.
async fn build_configs_via_seam(
    app_state: &Entity<AppStateEntity>,
    source_profile_id: Uuid,
    source_database: &str,
    target_profile_id: Uuid,
    target_database: &str,
    tables: &[TableRef],
    cx: &mut AsyncApp,
) -> Result<Vec<TableMigrationConfig>, String> {
    let mut configs = Vec::with_capacity(tables.len());

    for table_ref in tables {
        let source_info = match fetch_table_details_via_seam(
            app_state,
            source_profile_id,
            source_database,
            table_ref,
            cx,
        )
        .await
        {
            Ok(TableDetailsFetch::Found(info)) => info,
            Ok(TableDetailsFetch::NotFound(e)) | Err(e) => {
                return Err(format!("{}: {e}", table_ref.qualified_name()));
            }
        };
        let source_columns = to_transfer_columns(source_info.columns.unwrap_or_default());

        let target_fetch = match fetch_table_details_via_seam(
            app_state,
            target_profile_id,
            target_database,
            table_ref,
            cx,
        )
        .await
        {
            Ok(fetch) => fetch,
            Err(e) => return Err(format!("{}: {e}", table_ref.qualified_name())),
        };

        // An Ok fetch that carries no columns means the target relation does
        // not exist (Postgres/MySQL report a missing table this way instead of
        // ObjectNotFound), so it is treated the same as NotFound: the row is a
        // Create that mirrors the source columns, not an Existing target.
        let (target_exists, target_columns) = match target_fetch {
            TableDetailsFetch::Found(info)
                if target_columns_prove_existing(info.columns.as_deref()) =>
            {
                (true, to_transfer_columns(info.columns.unwrap_or_default()))
            }
            _ => (false, Vec::new()),
        };

        configs.push(TableMigrationConfig::new(
            table_ref.clone(),
            source_columns,
            target_exists,
            target_columns,
        ));
    }

    Ok(configs)
}

/// The phase reached by pressing the footer's Continue button from `phase`,
/// or `None` for phases whose forward action lives elsewhere: `Confirm` starts
/// the run through the Confirm/Run phase's own "Start Migration" button, and
/// `Run` has no forward step. Pure so the footer's button visibility and the
/// dispatch in [`MigrateWizard::advance`] agree by construction.
fn next_phase(phase: WizardPhase) -> Option<WizardPhase> {
    match phase {
        WizardPhase::SourceTarget => Some(WizardPhase::TablesMapping),
        WizardPhase::TablesMapping => Some(WizardPhase::Options),
        WizardPhase::Options => Some(WizardPhase::Confirm),
        WizardPhase::Confirm | WizardPhase::Run => None,
    }
}

/// The phase reached by pressing the footer's Back button from `phase`, or
/// `None` for the first phase and for `Run` (whose back-navigation is frozen —
/// leaving a live or finished run behind is handled by Cancel/Close, not Back).
fn prev_phase(phase: WizardPhase) -> Option<WizardPhase> {
    match phase {
        WizardPhase::SourceTarget | WizardPhase::Run => None,
        WizardPhase::TablesMapping => Some(WizardPhase::SourceTarget),
        WizardPhase::Options => Some(WizardPhase::TablesMapping),
        WizardPhase::Confirm => Some(WizardPhase::Options),
    }
}

/// The migration wizard modal: owns the current [`WizardPhase`] plus the four
/// child phase entities it mounts as the user advances. Downstream phases are
/// invalidated (dropped) whenever an upstream phase reports a change, so a
/// re-advance rebuilds them against fresh inputs; navigating back and forward
/// without changes reuses the already-built entities.
pub struct MigrateWizard {
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    visible: bool,

    source_profile_id: Option<Uuid>,
    source_database: Option<String>,
    source_tables: Vec<TableRef>,

    phase: WizardPhase,
    error: Option<String>,
    advancing: bool,

    /// Monotonic epoch bumped whenever the inputs an in-flight advance was
    /// spawned against are invalidated (selection/mapping/options edits,
    /// re-open). Each advance spawn captures the value at spawn time and
    /// discards its result if the epoch moved — a stale completion must never
    /// mount a phase built from abandoned inputs or force a phase jump.
    generation: u64,

    /// The effective Source & Target selection (checked tables + target
    /// container) the downstream phases were last built from. Used to ignore
    /// no-op `SourceTargetChanged` events (re-selecting the same target,
    /// toggling a check off and back on) so mapping/options work is only
    /// discarded on a real change.
    built_selection: Option<(Vec<TableRef>, Uuid, String)>,

    /// The effective selection captured when the currently in-flight
    /// `Source & Target` advance was spawned. While an advance is in flight it
    /// is judged against this snapshot — not [`Self::built_selection`] — so a
    /// completing background tree fetch that re-emits `SourceTargetChanged`
    /// without changing the selection (a no-op) does not cancel the advance the
    /// user already triggered. `None` when no advance is in flight.
    advance_selection: Option<(Vec<TableRef>, Uuid, String)>,

    /// The phase whose child tree/grid currently holds keyboard focus. Drives
    /// the render-time focus routing so the active phase receives arrow-key
    /// focus on entry without a click-in first (keyboard-first identity).
    focused_phase: Option<WizardPhase>,

    /// Resolved once the `Source & Target` phase is left, then fed to the
    /// downstream phases and the run without re-deriving them.
    resolved_source_database: Option<String>,
    target_profile_id: Option<Uuid>,
    target_database: Option<String>,
    supports_truncate: bool,
    supports_disable_ri: bool,

    source_target: Option<Entity<SourceTargetPhase>>,
    mapping: Option<Entity<MappingPhase>>,
    options: Option<Entity<OptionsPhase>>,
    confirm_run: Option<Entity<ConfirmRunPhase>>,

    _source_target_sub: Option<Subscription>,
    _mapping_sub: Option<Subscription>,
    _options_sub: Option<Subscription>,
    _confirm_run_sub: Option<Subscription>,
}

impl MigrateWizard {
    pub fn new(app_state: Entity<AppStateEntity>, cx: &mut Context<Self>) -> Self {
        Self {
            app_state,
            focus_handle: cx.focus_handle(),
            visible: false,
            source_profile_id: None,
            source_database: None,
            source_tables: Vec::new(),
            phase: WizardPhase::SourceTarget,
            error: None,
            advancing: false,
            generation: 0,
            built_selection: None,
            advance_selection: None,
            focused_phase: None,
            resolved_source_database: None,
            target_profile_id: None,
            target_database: None,
            supports_truncate: false,
            supports_disable_ri: false,
            source_target: None,
            mapping: None,
            options: None,
            confirm_run: None,
            _source_target_sub: None,
            _mapping_sub: None,
            _options_sub: None,
            _confirm_run_sub: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(
        &mut self,
        source_profile_id: Uuid,
        source_database: Option<String>,
        source_tables: Vec<TableRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // A migration already in flight owns the wizard until it terminates.
        // Re-entering would drop the run's owner (orphaning its task and
        // progress) and could start a second concurrent migration, so instead
        // surface the in-progress run and ignore the new request.
        if self.is_running(cx) {
            self.visible = true;
            self.phase = WizardPhase::Run;
            self.focus_handle.focus(window);
            cx.notify();
            return;
        }

        self.visible = true;
        self.source_profile_id = Some(source_profile_id);
        self.source_database = source_database.clone();
        self.source_tables = source_tables.clone();
        self.phase = WizardPhase::SourceTarget;
        self.error = None;
        self.advancing = false;
        self.generation = self.generation.wrapping_add(1);
        self.built_selection = None;
        self.advance_selection = None;
        self.focused_phase = None;

        self.resolved_source_database = None;
        self.target_profile_id = None;
        self.target_database = None;
        self.supports_truncate = false;
        self.supports_disable_ri = false;

        self.mapping = None;
        self.options = None;
        self.confirm_run = None;
        self._mapping_sub = None;
        self._options_sub = None;
        self._confirm_run_sub = None;

        let app_state = self.app_state.clone();
        let source_target = cx.new(|cx| {
            SourceTargetPhase::new(
                app_state,
                source_profile_id,
                source_database,
                source_tables,
                cx,
            )
        });
        self._source_target_sub = Some(cx.subscribe(
            &source_target,
            |this, _entity, _event: &SourceTargetChanged, cx| {
                this.on_source_target_changed(cx);
            },
        ));
        self.source_target = Some(source_target);

        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.notify();
    }

    /// A changed source selection or target container invalidates every
    /// downstream phase — the mapping configs, the options' capability
    /// gating, and the confirm/run plan all derive from it. Invalidation is
    /// deferred to the next advance (see [`Self::advance_from_source_target`])
    /// so a no-op round trip — a check toggled off and back on, the same
    /// target re-selected — never discards the user's mapping/options work.
    /// A real deviation still orphans any in-flight advance immediately: its
    /// result was spawned against inputs that no longer hold.
    fn on_source_target_changed(&mut self, cx: &mut Context<Self>) {
        // A live run owns the wizard; a late tree-fetch completion must not
        // disturb it (and there is nothing downstream to rebuild).
        if self.is_running(cx) {
            return;
        }

        // While a Source & Target advance is in flight, judge the event against
        // the selection that advance was spawned with — not the built selection
        // — so a completing background tree fetch that re-emits
        // `SourceTargetChanged` without changing the selection does not cancel
        // the advance the user already triggered. A genuine change still
        // differs and cancels. With no such advance in flight
        // (`advance_selection` is `None`, including during the later
        // `Options → Confirm` advance) the built selection is the reference.
        let reference = if self.advance_selection.is_some() {
            &self.advance_selection
        } else {
            &self.built_selection
        };
        if !self.selection_matches(reference, cx) {
            self.invalidate_in_flight_advance();
        }
        cx.notify();
    }

    /// Whether the confirm/run phase currently owns a migration in the
    /// `Running` state. Drives the guards that keep the run's owner alive and
    /// the rail frozen on `Run` for the duration of the run.
    fn is_running(&self, cx: &App) -> bool {
        self.confirm_run
            .as_ref()
            .is_some_and(|phase| phase.read(cx).run_state() == RunState::Running)
    }

    /// Whether the Source & Target phase's current effective selection equals a
    /// previously captured one (the built selection, or the selection an
    /// in-flight advance was spawned with). `None` — no captured selection —
    /// never matches, so the caller treats it as a change.
    fn selection_matches(
        &self,
        captured: &Option<(Vec<TableRef>, Uuid, String)>,
        cx: &App,
    ) -> bool {
        let Some((tables, profile_id, database)) = captured.as_ref() else {
            return false;
        };
        let Some(source_target) = self.source_target.as_ref() else {
            return false;
        };

        let phase = source_target.read(cx);
        phase.checked_source_tables() == *tables
            && phase.target_profile_id() == Some(*profile_id)
            && phase.target_database().as_deref() == Some(database.as_str())
    }

    /// Orphans any in-flight advance spawn: bumping the generation makes its
    /// completion discard itself, and the footer stops showing "Loading…" for
    /// work whose inputs no longer exist.
    fn invalidate_in_flight_advance(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.advancing = false;
        self.advance_selection = None;
    }

    /// Edited mappings only invalidate the confirm/run plan and its FK order;
    /// the options phase is independent of the per-table mapping.
    fn on_mapping_changed(&mut self, cx: &mut Context<Self>) {
        // Never drop the confirm/run phase while it owns a live run — that
        // would orphan the migration. Edits are impossible during a run anyway
        // (the rail is frozen on Run), but a late async reseed could still emit.
        if self.is_running(cx) {
            return;
        }
        self.invalidate_in_flight_advance();
        self.confirm_run = None;
        self._confirm_run_sub = None;
        cx.notify();
    }

    fn on_options_changed(&mut self, cx: &mut Context<Self>) {
        if self.is_running(cx) {
            return;
        }
        self.invalidate_in_flight_advance();
        self.confirm_run = None;
        self._confirm_run_sub = None;
        cx.notify();
    }

    fn on_confirm_run_event(&mut self, event: &ConfirmRunEvent, cx: &mut Context<Self>) {
        match event {
            ConfirmRunEvent::RunStarted => {
                self.phase = WizardPhase::Run;
                cx.notify();
            }
            ConfirmRunEvent::CloseRequested => self.close(cx),
        }
    }

    /// Resolves the source connection, honoring the specific database the
    /// sidebar selection was opened from (for `ConnectionPerDatabase`
    /// drivers), falling back to the profile's primary connection.
    fn resolve_source_connection(&self, cx: &App) -> Option<Arc<dyn Connection>> {
        let profile_id = self.source_profile_id?;
        let connected = self.app_state.read(cx).connections().get(&profile_id)?;
        Some(match &self.source_database {
            Some(db) => connected.connection_for_database(db),
            None => connected.connection.clone(),
        })
    }

    /// Resolves the target connection scoped to the chosen target database
    /// (for `ConnectionPerDatabase` drivers), mirroring
    /// [`Self::resolve_source_connection`] and the import wizard — the
    /// profile's primary connection may be bound to a different database, and
    /// the sink would silently write the migrated rows there.
    fn resolve_target_connection(
        &self,
        profile_id: Uuid,
        target_database: &str,
        cx: &App,
    ) -> Option<Arc<dyn Connection>> {
        let connected = self.app_state.read(cx).connections().get(&profile_id)?;
        Some(connected.connection_for_database(target_database))
    }

    /// A human-readable `profile / database` label for the Confirm summary.
    fn container_label(&self, profile_id: Uuid, database: &str, cx: &App) -> String {
        let name = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_default();

        if database.is_empty() {
            name
        } else {
            format!("{name} / {database}")
        }
    }

    /// Footer Back button: steps one phase backwards through the linear flow.
    /// Shares [`Self::go_to_phase`]'s running guard, so it is inert during a
    /// live run.
    fn go_back(&mut self, cx: &mut Context<Self>) {
        if let Some(previous) = prev_phase(self.phase) {
            self.go_to_phase(previous, cx);
        }
    }

    /// Footer Cancel button: signals the confirm/run phase's cancel token so
    /// the in-flight migration stops at the next chunk boundary.
    fn cancel_run(&mut self, cx: &mut Context<Self>) {
        if let Some(phase) = self.confirm_run.clone() {
            phase.update(cx, |phase, cx| phase.cancel_run(cx));
        }
    }

    /// Footer Close button (shown once the run is `Done`): routes through the
    /// confirm/run phase's existing `CloseRequested` event so the single close
    /// path in the wizard's subscription stays the only way the modal dismisses.
    fn request_close(&mut self, cx: &mut Context<Self>) {
        match self.confirm_run.clone() {
            Some(phase) => {
                phase.update(cx, |_phase, cx| cx.emit(ConfirmRunEvent::CloseRequested));
            }
            None => self.close(cx),
        }
    }

    /// Back-navigation from the rail: only ever returns to an already-passed
    /// phase, keeping the downstream entities intact so re-advancing is free.
    fn go_to_phase(&mut self, phase: WizardPhase, cx: &mut Context<Self>) {
        // The rail is frozen on `Run` while a migration is live: navigating
        // back would drop the run's owner and orphan the in-flight task.
        if self.phase == WizardPhase::Run && self.is_running(cx) {
            return;
        }
        if phase < self.phase {
            self.phase = phase;
            cx.notify();
        }
    }

    /// Whether the footer's Continue button is enabled for the current phase:
    /// the `Source & Target` and `Tables Mapping` guards compose their child's
    /// readiness; `Options` always has a usable value.
    fn continue_enabled(&self, cx: &App) -> bool {
        match self.phase {
            WizardPhase::SourceTarget => self
                .source_target
                .as_ref()
                .is_some_and(|phase| phase.read(cx).is_ready(cx)),
            WizardPhase::TablesMapping => self
                .mapping
                .as_ref()
                .is_some_and(|phase| phase.read(cx).can_advance()),
            WizardPhase::Options => true,
            WizardPhase::Confirm | WizardPhase::Run => false,
        }
    }

    fn advance(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.phase {
            WizardPhase::SourceTarget => self.advance_from_source_target(window, cx),
            WizardPhase::TablesMapping => self.advance_from_tables_mapping(window, cx),
            WizardPhase::Options => self.advance_from_options(cx),
            WizardPhase::Confirm | WizardPhase::Run => {}
        }
    }

    fn report_advance_error(
        &mut self,
        kind: ErrorKind,
        message: impl Into<String>,
        cx: &mut Context<Self>,
    ) {
        let message = message.into();
        self.advancing = false;
        self.error = Some(message.clone());
        report_error(UserFacingError::new(kind, message), cx);
        cx.notify();
    }

    /// `Source & Target` → `Tables Mapping`: resolves the chosen connections,
    /// then (re)builds the mapping grid off-thread from the checked tables'
    /// schemas and the target container's existing tables.
    fn advance_from_source_target(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(source_target) = self.source_target.as_ref() else {
            return;
        };
        let source_target = source_target.read(cx);
        if !source_target.is_ready(cx) {
            return;
        }

        let checked = source_target.checked_source_tables();
        let resolved_source_database = source_target.source_database().to_string();
        let Some(target_profile_id) = source_target.target_profile_id() else {
            return;
        };
        let Some(target_database) = source_target.target_database() else {
            return;
        };
        let Some(source_profile_id) = self.source_profile_id else {
            return;
        };

        let Some(source_connection) = self.resolve_source_connection(cx) else {
            self.report_advance_error(
                ErrorKind::Storage,
                "No active connection for the source profile",
                cx,
            );
            return;
        };
        let Some(target_connection) =
            self.resolve_target_connection(target_profile_id, &target_database, cx)
        else {
            self.report_advance_error(
                ErrorKind::Storage,
                "No active connection for the target profile",
                cx,
            );
            return;
        };

        // Bind the plan's source database to exactly the database the phase
        // resolved the checked tables against, so every checked table lines up
        // with the single source the engine reads from. Fall back to the
        // live connection only when the phase could not resolve one.
        let source_database = if resolved_source_database.is_empty() {
            source_connection.active_database().unwrap_or_default()
        } else {
            resolved_source_database
        };

        self.resolved_source_database = Some(source_database.clone());
        self.target_profile_id = Some(target_profile_id);
        self.target_database = Some(target_database.clone());
        self.supports_truncate = target_connection.supports(DriverCapabilities::TRUNCATE_TABLE);
        self.supports_disable_ri =
            target_connection.supports(DriverCapabilities::DISABLE_FK_CHECKS);

        // Deferred invalidation of downstream phases: only a selection that
        // actually differs from the one they were built from discards them —
        // a no-op round trip through the trees keeps the mapping work intact.
        let new_selection = (checked.clone(), target_profile_id, target_database.clone());
        if self.built_selection.as_ref() != Some(&new_selection) {
            self.mapping = None;
            self._mapping_sub = None;
            self.options = None;
            self._options_sub = None;
            self.confirm_run = None;
            self._confirm_run_sub = None;
        }
        self.built_selection = Some(new_selection);

        if self.mapping.is_some() {
            self.phase = WizardPhase::TablesMapping;
            cx.notify();
            return;
        }

        self.advancing = true;
        self.advance_selection = self.built_selection.clone();
        self.error = None;
        cx.notify();

        let app_state = self.app_state.clone();
        let supports_truncate = self.supports_truncate;
        let list_connection = Arc::clone(&target_connection);
        let list_database = target_database.clone();
        let mapping_source_database = source_database.clone();
        let generation = self.generation;

        cx.spawn_in(window, async move |this, cx| {
            let configs = build_configs_via_seam(
                &app_state,
                source_profile_id,
                &source_database,
                target_profile_id,
                &target_database,
                &checked,
                cx,
            )
            .await;

            let existing = cx
                .background_executor()
                .spawn(async move { list_connection.schema_for_database(&list_database) })
                .await;

            this.update_in(cx, |this, window, cx| {
                if this.generation != generation {
                    return;
                }

                this.advancing = false;
                this.advance_selection = None;
                match configs {
                    Ok(configs) => {
                        // A failed listing only affects Create/Existing
                        // classification while the user types a new name, so
                        // the empty-list fallback is safe — but the error is
                        // traced rather than silently dropped.
                        let existing_target_tables = existing
                            .map(|info| {
                                info.tables
                                    .into_iter()
                                    .map(|table| table.name)
                                    .collect::<Vec<_>>()
                            })
                            .log_err_with("Could not list existing target tables for mapping")
                            .unwrap_or_default();
                        this.mount_mapping(
                            configs,
                            existing_target_tables,
                            supports_truncate,
                            source_profile_id,
                            mapping_source_database,
                            target_profile_id,
                            target_database,
                            window,
                            cx,
                        );
                        this.phase = WizardPhase::TablesMapping;
                        cx.notify();
                    }
                    Err(e) => {
                        this.report_advance_error(
                            ErrorKind::Driver,
                            format!("Could not read table schema: {e}"),
                            cx,
                        );
                    }
                }
            })
            .ok();
        })
        .detach();
    }

    #[allow(clippy::too_many_arguments)]
    fn mount_mapping(
        &mut self,
        configs: Vec<TableMigrationConfig>,
        existing_target_tables: Vec<String>,
        supports_truncate: bool,
        source_profile_id: Uuid,
        source_database: String,
        target_profile_id: Uuid,
        target_database: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let app_state = self.app_state.clone();
        let mapping = cx.new(|cx| {
            MappingPhase::new(
                app_state,
                source_profile_id,
                source_database,
                target_profile_id,
                target_database,
                existing_target_tables,
                supports_truncate,
                configs,
                window,
                cx,
            )
        });
        self._mapping_sub = Some(
            cx.subscribe(&mapping, |this, _entity, _event: &MappingChanged, cx| {
                this.on_mapping_changed(cx)
            }),
        );
        self.mapping = Some(mapping);
    }

    fn advance_from_tables_mapping(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(mapping) = self.mapping.as_ref() else {
            return;
        };
        if !mapping.read(cx).can_advance() {
            return;
        }

        if self.options.is_none() {
            let supports_disable_ri = self.supports_disable_ri;
            let options = cx.new(|cx| OptionsPhase::new(supports_disable_ri, window, cx));
            self._options_sub = Some(
                cx.subscribe(&options, |this, _entity, _event: &OptionsChanged, cx| {
                    this.on_options_changed(cx)
                }),
            );
            self.options = Some(options);
        }

        self.phase = WizardPhase::Options;
        self.error = None;
        cx.notify();
    }

    /// `Options` → `Confirm`: fetches the selected tables' foreign keys through
    /// the shared seam and computes the load order off-thread, surfacing a
    /// reorder interrupt on a cycle (via [`decide_order`]) before mounting the
    /// Confirm/Run phase.
    fn advance_from_options(&mut self, cx: &mut Context<Self>) {
        if self.confirm_run.is_some() {
            self.phase = WizardPhase::Confirm;
            cx.notify();
            return;
        }

        let Some(mapping) = self.mapping.as_ref() else {
            return;
        };
        let Some(options) = self.options.as_ref() else {
            return;
        };
        let Some(target_profile_id) = self.target_profile_id else {
            return;
        };
        let Some(target_database) = self.target_database.clone() else {
            return;
        };
        let Some(source_profile_id) = self.source_profile_id else {
            return;
        };

        let configs: Vec<TableMigrationConfig> = mapping.read(cx).configs().cloned().collect();
        let segment_size = options.read(cx).segment_size();
        let disable_referential_integrity = options.read(cx).disable_referential_integrity();

        let Some(source_connection) = self.resolve_source_connection(cx) else {
            self.report_advance_error(
                ErrorKind::Storage,
                "No active connection for the source profile",
                cx,
            );
            return;
        };
        let Some(target_connection) =
            self.resolve_target_connection(target_profile_id, &target_database, cx)
        else {
            self.report_advance_error(
                ErrorKind::Storage,
                "No active connection for the target profile",
                cx,
            );
            return;
        };

        let source_database = self
            .resolved_source_database
            .clone()
            .or_else(|| self.source_database.clone())
            .or_else(|| source_connection.active_database())
            .unwrap_or_default();

        let source_container_label = self.container_label(source_profile_id, &source_database, cx);
        let target_container_label = self.container_label(target_profile_id, &target_database, cx);

        // A non-destructive row whose target is one of the source tables in the
        // same container appends into a table that is also being read; that is
        // allowed but worth flagging on the Confirm screen (the destructive
        // variant is already blocked at the mapping step, LD-14).
        let same_container =
            source_profile_id == target_profile_id && source_database == target_database;
        let pre_run_warnings = {
            let plans: Vec<MappingRowPlan> = configs
                .iter()
                .map(|config| MappingRowPlan {
                    source_schema: config.source_table.schema.as_deref(),
                    source_table: config.source_table.name.as_str(),
                    target_schema: config.target_schema.as_deref(),
                    target_table: config.target_table.as_str(),
                    destructive: config.is_destructive(),
                })
                .collect();
            tables_mapping_confirm_warnings(&plans, same_container)
        };

        let table_refs: Vec<TableRef> = configs.iter().map(|c| c.source_table.clone()).collect();
        let mut schemas: Vec<Option<String>> =
            table_refs.iter().map(|t| t.schema.clone()).collect();
        schemas.sort();
        schemas.dedup();

        self.advancing = true;
        self.error = None;
        cx.notify();

        let app_state = self.app_state.clone();
        let generation = self.generation;

        cx.spawn(async move |this, cx| {
            let mut foreign_keys: Vec<SchemaForeignKeyInfo> = Vec::new();
            let mut fetch_error: Option<String> = None;

            for schema in &schemas {
                match fetch_schema_foreign_keys_via_seam(
                    &app_state,
                    source_profile_id,
                    &source_database,
                    schema.as_deref(),
                    cx,
                )
                .await
                {
                    Ok(batch) => foreign_keys.extend(batch),
                    Err(e) => {
                        fetch_error = Some(e);
                        break;
                    }
                }
            }

            let order_result = match fetch_error {
                Some(e) => Err(e),
                None => Ok(cx
                    .background_executor()
                    .spawn(async move { topological_order(&table_refs, &foreign_keys) })
                    .await),
            };

            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }

                this.advancing = false;
                match order_result {
                    Ok(order) => {
                        let inputs = ConfirmRunInputs {
                            app_state: this.app_state.clone(),
                            source_connection,
                            target_connection,
                            source_database,
                            target_database,
                            target_profile_id,
                            source_container_label,
                            target_container_label,
                            segment_size,
                            disable_referential_integrity,
                            order: decide_order(order),
                            configs,
                            pre_run_warnings,
                        };
                        this.mount_confirm_run(inputs, cx);
                        this.phase = WizardPhase::Confirm;
                        cx.notify();
                    }
                    Err(e) => {
                        this.report_advance_error(
                            ErrorKind::Driver,
                            format!("Could not read foreign keys: {e}"),
                            cx,
                        );
                    }
                }
            })
            .ok();
        })
        .detach();
    }

    /// The focus handle of the child entity backing the current phase, so the
    /// wizard can route keyboard focus into the active tree/grid on entry.
    /// `Confirm` and `Run` share the confirm/run child.
    fn active_phase_focus_handle(&self, cx: &App) -> Option<FocusHandle> {
        match self.phase {
            WizardPhase::SourceTarget => self
                .source_target
                .as_ref()
                .map(|entity| entity.read(cx).focus_handle().clone()),
            WizardPhase::TablesMapping => self
                .mapping
                .as_ref()
                .map(|entity| entity.read(cx).focus_handle().clone()),
            WizardPhase::Options => self
                .options
                .as_ref()
                .map(|entity| entity.read(cx).focus_handle().clone()),
            WizardPhase::Confirm | WizardPhase::Run => self
                .confirm_run
                .as_ref()
                .map(|entity| entity.read(cx).focus_handle().clone()),
        }
    }

    /// Moves keyboard focus into the active phase's child once per phase
    /// change, so arrow-key navigation works without clicking into the tree
    /// first. Focus stays put across re-renders of the same phase, so tabbing
    /// to the footer button is never stolen back.
    fn focus_active_phase_on_entry(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.focused_phase == Some(self.phase) {
            return;
        }
        if let Some(handle) = self.active_phase_focus_handle(cx) {
            handle.focus(window);
            self.focused_phase = Some(self.phase);
        }
    }

    fn mount_confirm_run(&mut self, inputs: ConfirmRunInputs, cx: &mut Context<Self>) {
        let confirm_run = cx.new(|cx| ConfirmRunPhase::new(inputs, cx));
        self._confirm_run_sub = Some(cx.subscribe(
            &confirm_run,
            |this, _entity, event: &ConfirmRunEvent, cx| this.on_confirm_run_event(event, cx),
        ));
        self.confirm_run = Some(confirm_run);
    }

    fn summarize(outcome: &dbflux_transfer::migration::MigrationRunOutcome) -> String {
        let completed = outcome
            .tables
            .iter()
            .filter(|t| matches!(t.status, TableTransferStatus::Completed { .. }))
            .count();
        let skipped = outcome
            .tables
            .iter()
            .filter(|t| matches!(t.status, TableTransferStatus::Skipped))
            .count();
        let failed = outcome
            .tables
            .iter()
            .filter(|t| matches!(t.status, TableTransferStatus::Failed { .. }))
            .count();
        let rows: u64 = outcome
            .tables
            .iter()
            .map(|t| match &t.status {
                TableTransferStatus::Completed { rows } => *rows,
                _ => 0,
            })
            .sum();

        if failed > 0 {
            format!(
                "Migrated {completed} table(s), {rows} row(s) total ({skipped} skipped, {failed} failed)"
            )
        } else {
            format!("Migrated {completed} table(s), {rows} row(s) total ({skipped} skipped)")
        }
    }

    /// Renders one status line per planned table when the run left any table
    /// `Failed` or `NotStarted`, so the user sees exactly which tables
    /// succeeded, which one failed with what error, and which were never
    /// attempted — not just the last error swallowed into a single toast
    /// (R4-002/B-007). On a fully successful/skipped run, only the engine's
    /// own warnings are shown, unchanged.
    fn itemized_status_lines(tables: &[MigratedTable], engine_warnings: &[String]) -> Vec<String> {
        let has_issue = tables.iter().any(|t| {
            matches!(
                t.status,
                TableTransferStatus::Failed { .. } | TableTransferStatus::NotStarted
            )
        });

        if !has_issue {
            return engine_warnings.to_vec();
        }

        let mut lines: Vec<String> = tables.iter().map(Self::table_status_line).collect();
        lines.extend(engine_warnings.iter().cloned());
        lines
    }

    fn table_status_line(table: &MigratedTable) -> String {
        match &table.status {
            TableTransferStatus::Completed { rows } => {
                format!("{}: completed ({rows} row(s))", table.source_table)
            }
            TableTransferStatus::Skipped => format!("{}: skipped", table.source_table),
            TableTransferStatus::Failed { error } => {
                format!("{}: FAILED — {error}", table.source_table)
            }
            TableTransferStatus::NotStarted => format!("{}: not attempted", table.source_table),
        }
    }
}

impl Render for MigrateWizard {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus_active_phase_on_entry(window, cx);

        let close_entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            close_entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let frame = ModalFrame::new("migrate-wizard", &self.focus_handle, close)
            .title("Migrate Data")
            .icon(AppIcon::ArrowUpDown)
            .width(px(1000.0))
            .height_fraction(0.8)
            .center_vertically()
            .child(self.render_body(cx));

        frame.render(cx).into_any_element()
    }
}

impl MigrateWizard {
    fn render_body(&self, cx: &mut Context<Self>) -> AnyElement {
        let rail_entity = cx.entity().downgrade();
        let on_select = move |index: usize, _window: &mut Window, app: &mut App| {
            let phase = RAIL_PHASES[index];
            rail_entity
                .update(app, |this, cx| this.go_to_phase(phase, cx))
                .ok();
        };

        // `flex_1` (not `size_full`): the modal container is a fixed-height
        // flex column whose first child is the header, so the body must grow
        // into the *remaining* height. `size_full` (100% height) would instead
        // push the body to the full container height below the header, and the
        // container's `overflow_hidden` would then clip the footer off-screen.
        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h(px(0.0))
            .w_full()
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_1()
                    .min_h(px(0.0))
                    .child(render_wizard_rail(
                        &to_rail_items(self.phase),
                        Some(on_select),
                        cx,
                    ))
                    .child(self.render_phase_area()),
            )
            .child(self.render_footer(cx))
            .into_any_element()
    }

    fn render_phase_area(&self) -> AnyElement {
        let content = match self.phase {
            WizardPhase::SourceTarget => self
                .source_target
                .as_ref()
                .map(|entity| entity.clone().into_any_element()),
            WizardPhase::TablesMapping => self
                .mapping
                .as_ref()
                .map(|entity| entity.clone().into_any_element()),
            WizardPhase::Options => self
                .options
                .as_ref()
                .map(|entity| entity.clone().into_any_element()),
            WizardPhase::Confirm | WizardPhase::Run => self
                .confirm_run
                .as_ref()
                .map(|entity| entity.clone().into_any_element()),
        };

        div()
            .flex_1()
            .min_w(px(0.0))
            .flex()
            .flex_col()
            .when_some(content, |parent, element| parent.child(element))
            .into_any_element()
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let border = theme.border;

        let run_state = self
            .confirm_run
            .as_ref()
            .map(|phase| phase.read(cx).run_state());
        let running = run_state == Some(RunState::Running);
        let done = run_state == Some(RunState::Done);

        let shows_back = prev_phase(self.phase).is_some() && !running && !done;
        let shows_continue = next_phase(self.phase).is_some();
        let continue_enabled = !self.advancing && self.continue_enabled(cx);
        let continue_label = if self.advancing {
            "Loading…"
        } else {
            "Continue"
        };

        let actions = div()
            .flex()
            .flex_row()
            .items_center()
            .gap(Spacing::SM)
            .when(shows_back, |parent| {
                parent.child(
                    Button::new("migrate-wizard-back", "Back")
                        .small()
                        .ghost()
                        .disabled(self.advancing)
                        .on_click(cx.listener(|this, _event, _window, cx| this.go_back(cx))),
                )
            })
            .when(shows_continue, |parent| {
                parent.child(
                    Button::new("migrate-wizard-continue", continue_label)
                        .small()
                        .primary()
                        .disabled(!continue_enabled)
                        .on_click(cx.listener(|this, _event, window, cx| this.advance(window, cx))),
                )
            })
            .when(running, |parent| {
                parent.child(
                    Button::new("migrate-wizard-cancel", "Cancel")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _event, _window, cx| this.cancel_run(cx))),
                )
            })
            .when(done, |parent| {
                parent.child(
                    Button::new("migrate-wizard-close", "Close")
                        .small()
                        .primary()
                        .on_click(cx.listener(|this, _event, _window, cx| this.request_close(cx))),
                )
            });

        div()
            .flex()
            .flex_row()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .border_t_1()
            .border_color(border)
            .child(
                div()
                    .flex_1()
                    .min_w(px(0.0))
                    .when_some(self.error.clone(), |parent, error| {
                        parent.child(Text::caption(error).danger())
                    }),
            )
            .child(actions)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TableMigrationConfig, WizardPhase, build_migration_options, build_migration_table_plans,
        is_already_cached_sentinel, next_phase, target_columns_prove_existing,
    };
    use dbflux_core::{ColumnInfo, TableRef, TransferColumn};

    fn transfer_column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
        }
    }

    fn column_info(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: "text".to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
            enum_values: None,
        }
    }

    #[test]
    fn target_columns_prove_existing_only_for_a_non_empty_column_set() {
        // A real, existing relation always projects at least one column.
        let columns = vec![column_info("id")];
        assert!(target_columns_prove_existing(Some(&columns)));

        // Ok-but-empty columns (Postgres/MySQL "table absent" signal) and a
        // never-loaded column set both mean the target must be created.
        assert!(!target_columns_prove_existing(Some(&[])));
        assert!(!target_columns_prove_existing(None));
    }

    #[test]
    fn is_already_cached_sentinel_matches_only_the_known_sentinel_strings() {
        assert!(is_already_cached_sentinel("Table details already cached"));
        assert!(is_already_cached_sentinel(
            "Schema foreign keys already cached"
        ));
        assert!(!is_already_cached_sentinel("Profile not connected"));
        assert!(!is_already_cached_sentinel(""));
    }

    #[test]
    fn next_phase_advances_through_the_forward_flow_and_stops_at_confirm() {
        assert_eq!(
            next_phase(WizardPhase::SourceTarget),
            Some(WizardPhase::TablesMapping)
        );
        assert_eq!(
            next_phase(WizardPhase::TablesMapping),
            Some(WizardPhase::Options)
        );
        assert_eq!(next_phase(WizardPhase::Options), Some(WizardPhase::Confirm));
        assert_eq!(next_phase(WizardPhase::Confirm), None);
        assert_eq!(next_phase(WizardPhase::Run), None);
    }

    #[test]
    fn build_migration_table_plans_maps_every_config_field_and_defaults_estimate_to_none() {
        let source_columns = vec![transfer_column("id"), transfer_column("legacy_x")];
        let target_columns = vec![transfer_column("id"), transfer_column("y")];
        let mut config = TableMigrationConfig::new(
            TableRef::new("users"),
            source_columns.clone(),
            true,
            target_columns,
        );
        config.target_schema = Some("public".to_string());
        config.set_binding(1, Some(1));
        let expected_overrides = config.to_overrides();

        let plans = build_migration_table_plans(std::slice::from_ref(&config));

        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert_eq!(plan.source_table, config.source_table);
        assert_eq!(plan.source_columns, source_columns);
        assert_eq!(plan.target_schema, Some("public".to_string()));
        assert_eq!(plan.target_table, config.target_table);
        assert_eq!(plan.mapping_mode, config.mapping_mode);
        assert_eq!(plan.column_overrides, Some(expected_overrides));
        assert_eq!(plan.estimated_total, None);
    }

    #[test]
    fn build_migration_table_plans_assembles_one_plan_per_config_in_order() {
        let users = TableMigrationConfig::new(
            TableRef::new("users"),
            vec![transfer_column("id")],
            true,
            vec![transfer_column("id")],
        );
        let orders = TableMigrationConfig::new(
            TableRef::new("orders"),
            vec![transfer_column("id")],
            false,
            Vec::new(),
        );
        let configs = vec![users, orders];

        let plans = build_migration_table_plans(&configs);

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].target_table, "users");
        assert_eq!(plans[1].target_table, "orders");
    }

    #[test]
    fn build_migration_options_maps_every_field_including_target_database() {
        let options = build_migration_options(
            500,
            "source_db".to_string(),
            "target_db".to_string(),
            true,
            false,
            Some(vec![TableRef::new("a"), TableRef::new("b")]),
        );

        assert_eq!(options.segment_size, 500);
        assert_eq!(options.source_database, "source_db");
        assert_eq!(options.target_database, "target_db");
        assert!(options.destructive_confirmed);
        assert!(!options.disable_referential_integrity);
        assert_eq!(
            options.manual_order,
            Some(vec![TableRef::new("a"), TableRef::new("b")])
        );
    }

    #[test]
    fn build_migration_options_with_no_manual_order_leaves_it_none() {
        let options =
            build_migration_options(250, "src".to_string(), "dst".to_string(), false, true, None);

        assert_eq!(options.manual_order, None);
        assert!(options.disable_referential_integrity);
    }
}
