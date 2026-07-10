//! T27 — Migrate wizard: pick a connected, transfer-compatible target
//! connection, review/adjust each table's target-table handling and column
//! mapping (mirroring the Import wizard's T22 review step), resolve FK load
//! order (R6) — surfacing a manual-reorder step on a cycle instead of
//! guessing — confirm any destructive plan, then run the migration via
//! `dbflux_transfer::migration::run_migration`.
//!
//! Reached from the sidebar's multi-select "Migrate…" action (T28), which
//! pre-populates `source_profile_id` / `source_database` / `source_tables`.

mod column_mapping;

use std::sync::{Arc, Mutex};

use dbflux_components::controls::{
    Button, Checkbox, Dropdown, DropdownItem, DropdownSelectionChanged,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::{
    Connection, DriverCapabilities, SchemaForeignKeyInfo, TableRef, TaskId, TaskKind, TaskStatus,
    TaskTarget, TransferColumn, topological_order,
};
use dbflux_transfer::TableTransferStatus;
use dbflux_transfer::migration::{
    MigratedTable, MigrationOptions, MigrationOutcome, MigrationTablePlan, run_migration,
};
use dbflux_ui_base::app_state_entity::{AppStateChanged, AppStateEntity};
use dbflux_ui_base::modal_frame::ModalFrame;
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;

pub use column_mapping::TableMigrationConfig;
use column_mapping::mapping_mode_options;

#[derive(Clone, Copy, PartialEq, Eq)]
enum WizardStep {
    PickTarget,
    Configure,
    ReorderCycle,
    Confirm,
    Running,
    Done,
}

/// One table row's live controls, wrapping the pure [`TableMigrationConfig`]
/// with the `Dropdown` entities the user adjusts it through — same shape as
/// the Import wizard's `TableImportRow`.
struct TableMigrationRow {
    config: TableMigrationConfig,
    mapping_mode_dropdown: Entity<Dropdown>,
    rebind_target_dropdown: Entity<Dropdown>,
    rebind_source_dropdown: Entity<Dropdown>,
}

pub struct MigrateWizard {
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    visible: bool,
    source_profile_id: Option<Uuid>,
    source_database: Option<String>,
    source_tables: Vec<TableRef>,
    step: WizardStep,
    error: Option<String>,
    target_dropdown: Entity<Dropdown>,
    /// Profile ids of connected, transfer-compatible targets, parallel to
    /// `target_dropdown`'s items — rebuilt once per `open()` call.
    target_candidates: Vec<Uuid>,
    target_profile_id: Option<Uuid>,
    rows: Vec<TableMigrationRow>,
    supports_truncate: bool,
    supports_disable_ri: bool,
    disable_ri: bool,
    /// The FK-ordered prefix that resolved cleanly (fixed, not reorderable)
    /// when a cycle was detected among the remaining tables.
    cyclic_prefix: Vec<TableRef>,
    /// The unresolved cyclic subset, in the user's current (reorderable)
    /// order — seeded from `topological_order`'s `cycle`.
    reorder_list: Vec<TableRef>,
    /// The final load order once resolved (auto or user-reordered),
    /// consumed verbatim by `run_migration`.
    final_order: Option<Vec<TableRef>>,
    /// Set only by the Confirm step's "Yes, proceed" handler — never derived
    /// from "is any plan destructive" — so the engine's destructive-confirm
    /// gate stays a real backstop against a state-machine bypass, not merely
    /// a restatement of the plan's own classification.
    confirmed_destructive: bool,
    loading: bool,
    running: bool,
    progress: Arc<Mutex<(u64, Option<u64>)>>,
    active_task_id: Option<TaskId>,
    result_summary: Option<String>,
    result_warnings: Vec<String>,
    _row_subscriptions: Vec<Subscription>,
    _target_subscription: Option<Subscription>,
}

impl MigrateWizard {
    pub fn new(app_state: Entity<AppStateEntity>, cx: &mut Context<Self>) -> Self {
        let target_dropdown =
            cx.new(|_cx| Dropdown::new("migrate-target").placeholder("Target connection"));

        Self {
            app_state,
            focus_handle: cx.focus_handle(),
            visible: false,
            source_profile_id: None,
            source_database: None,
            source_tables: Vec::new(),
            step: WizardStep::PickTarget,
            error: None,
            target_dropdown,
            target_candidates: Vec::new(),
            target_profile_id: None,
            rows: Vec::new(),
            supports_truncate: false,
            supports_disable_ri: false,
            disable_ri: false,
            cyclic_prefix: Vec::new(),
            reorder_list: Vec::new(),
            final_order: None,
            confirmed_destructive: false,
            loading: false,
            running: false,
            progress: Arc::new(Mutex::new((0, None))),
            active_task_id: None,
            result_summary: None,
            result_warnings: Vec::new(),
            _row_subscriptions: Vec::new(),
            _target_subscription: None,
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
        self.visible = true;
        self.source_profile_id = Some(source_profile_id);
        self.source_database = source_database;
        self.step = WizardStep::PickTarget;
        self.error = None;
        self.rows.clear();
        self._row_subscriptions.clear();
        self.cyclic_prefix.clear();
        self.reorder_list.clear();
        self.final_order = None;
        self.confirmed_destructive = false;
        self.disable_ri = false;
        self.loading = false;
        self.running = false;
        self.active_task_id = None;
        self.result_summary = None;
        self.result_warnings.clear();

        self.build_target_candidates(source_tables, cx);
        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.notify();
    }

    fn build_target_candidates(&mut self, source_tables: Vec<TableRef>, cx: &mut Context<Self>) {
        self.source_tables = source_tables;

        let state = self.app_state.read(cx);
        let Some(source_profile_id) = self.source_profile_id else {
            return;
        };
        let Some(source_connected) = state.connections().get(&source_profile_id) else {
            self.error = Some("No active connection for the selected tables".to_string());
            return;
        };
        let source_metadata = source_connected.connection.metadata();

        let mut candidates: Vec<(Uuid, String)> = state
            .connections()
            .iter()
            .filter(|(_, connected)| {
                dbflux_core::transfer_compatible(source_metadata, connected.connection.metadata())
            })
            .map(|(profile_id, connected)| (*profile_id, connected.profile.name.clone()))
            .collect();
        candidates.sort_by(|a, b| a.1.cmp(&b.1));

        self.target_candidates = candidates.iter().map(|(id, _)| *id).collect();
        let items: Vec<DropdownItem> = candidates
            .iter()
            .map(|(_, name)| DropdownItem::new(name.clone()))
            .collect();

        let target_dropdown = cx.new(|_cx| {
            Dropdown::new("migrate-target")
                .items(items)
                .placeholder("Target connection")
        });
        let subscription = cx.subscribe(
            &target_dropdown,
            |this, _entity, event: &DropdownSelectionChanged, cx| {
                this.target_profile_id = this.target_candidates.get(event.index).copied();
                cx.notify();
            },
        );
        self.target_dropdown = target_dropdown;
        self._target_subscription = Some(subscription);
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

    /// Resolves a candidate target connection by profile id, using its own
    /// primary connection — Migration does not offer a separate
    /// target-database sub-picker in this slice (T27 explicit scope).
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

    fn continue_from_pick_target(&mut self, cx: &mut Context<Self>) {
        let Some(target_profile_id) = self.target_profile_id else {
            self.error = Some("Choose a target connection".to_string());
            cx.notify();
            return;
        };
        let Some(source_connection) = self.resolve_source_connection(cx) else {
            self.error = Some("No active connection for the source profile".to_string());
            cx.notify();
            return;
        };
        let Some(target_connection) = self.resolve_target_connection(target_profile_id, cx) else {
            self.error = Some("No active connection for the target profile".to_string());
            cx.notify();
            return;
        };

        let source_database = self
            .source_database
            .clone()
            .or_else(|| source_connection.active_database())
            .unwrap_or_default();
        let target_database = target_connection.active_database().unwrap_or_default();
        let source_tables = self.source_tables.clone();
        let supports_truncate = target_connection.supports(DriverCapabilities::TRUNCATE_TABLE);
        let supports_disable_ri = target_connection.supports(DriverCapabilities::DISABLE_FK_CHECKS);

        self.loading = true;
        self.error = None;
        cx.notify();

        cx.spawn(async move |this, cx| {
            let build_result = cx
                .background_executor()
                .spawn(async move {
                    let mut configs = Vec::with_capacity(source_tables.len());
                    for table_ref in &source_tables {
                        let source_columns: Vec<TransferColumn> = source_connection
                            .table_details(
                                &source_database,
                                table_ref.schema.as_deref(),
                                &table_ref.name,
                            )
                            .map_err(|e| format!("{}: {e}", table_ref.qualified_name()))?
                            .columns
                            .unwrap_or_default()
                            .into_iter()
                            .map(|c| TransferColumn {
                                name: c.name,
                                type_name: Some(c.type_name),
                                nullable: c.nullable,
                                is_primary_key: c.is_primary_key,
                            })
                            .collect();

                        let target_lookup = target_connection.table_details(
                            &target_database,
                            table_ref.schema.as_deref(),
                            &table_ref.name,
                        );
                        let target_exists = target_lookup.is_ok();
                        let target_columns = target_lookup
                            .ok()
                            .and_then(|info| info.columns)
                            .unwrap_or_default()
                            .into_iter()
                            .map(|c| TransferColumn {
                                name: c.name,
                                type_name: Some(c.type_name),
                                nullable: c.nullable,
                                is_primary_key: c.is_primary_key,
                            })
                            .collect::<Vec<_>>();

                        configs.push(TableMigrationConfig::new(
                            table_ref.clone(),
                            source_columns,
                            target_exists,
                            target_columns,
                        ));
                    }

                    Ok::<Vec<TableMigrationConfig>, String>(configs)
                })
                .await;

            this.update(cx, |this, cx| {
                this.loading = false;
                match build_result {
                    Ok(configs) => {
                        this.supports_truncate = supports_truncate;
                        this.supports_disable_ri = supports_disable_ri;
                        this.build_rows(configs, cx);
                        this.step = WizardStep::Configure;
                    }
                    Err(e) => {
                        this.error = Some(format!("Could not read table schema: {e}"));
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn build_rows(&mut self, configs: Vec<TableMigrationConfig>, cx: &mut Context<Self>) {
        self.rows.clear();
        self._row_subscriptions.clear();
        let supports_truncate = self.supports_truncate;

        for (table_index, config) in configs.into_iter().enumerate() {
            let mode_options = mapping_mode_options(supports_truncate);
            let selected_mode_index = mode_options
                .iter()
                .position(|(_, mode)| *mode == config.mapping_mode);
            let mode_items: Vec<DropdownItem> = mode_options
                .iter()
                .map(|(label, _)| DropdownItem::new(*label))
                .collect();

            let mapping_mode_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!("migrate-mode-{table_index}")))
                    .items(mode_items)
                    .selected_index(selected_mode_index)
                    .placeholder("Mode")
            });

            let target_items: Vec<DropdownItem> = config
                .target_columns
                .iter()
                .map(|c| DropdownItem::new(c.name.clone()))
                .collect();
            let rebind_target_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!(
                    "migrate-target-col-{table_index}"
                )))
                .items(target_items)
                .placeholder("Target column")
            });

            let mut source_items = vec![DropdownItem::new("(unset)")];
            source_items.extend(
                config
                    .source_columns
                    .iter()
                    .map(|c| DropdownItem::new(c.name.clone())),
            );
            let rebind_source_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!(
                    "migrate-source-col-{table_index}"
                )))
                .items(source_items)
                .placeholder("Source column")
            });

            let mode_sub = cx.subscribe(
                &mapping_mode_dropdown,
                move |this, _entity, event: &DropdownSelectionChanged, cx| {
                    let mode_options = mapping_mode_options(this.supports_truncate);
                    if let Some((_, mode)) = mode_options.get(event.index)
                        && let Some(row) = this.rows.get_mut(table_index)
                    {
                        row.config.mapping_mode = *mode;
                        cx.notify();
                    }
                },
            );

            self.rows.push(TableMigrationRow {
                config,
                mapping_mode_dropdown,
                rebind_target_dropdown,
                rebind_source_dropdown,
            });
            self._row_subscriptions.push(mode_sub);
        }
    }

    fn apply_rebind(&mut self, table_index: usize, cx: &mut Context<Self>) {
        let Some(row) = self.rows.get(table_index) else {
            return;
        };

        let Some(target_index) = row
            .rebind_target_dropdown
            .read(cx)
            .selected_value()
            .and_then(|value| {
                row.config
                    .target_columns
                    .iter()
                    .position(|c| c.name.as_str() == value.as_ref())
            })
        else {
            return;
        };

        let source_index = row
            .rebind_source_dropdown
            .read(cx)
            .selected_value()
            .and_then(|value| {
                row.config
                    .source_columns
                    .iter()
                    .position(|c| c.name.as_str() == value.as_ref())
            });

        if let Some(row) = self.rows.get_mut(table_index) {
            row.config.set_binding(target_index, source_index);
        }
        cx.notify();
    }

    /// Resolves the FK load order over the selected tables. On a cycle,
    /// transitions to `ReorderCycle` instead of proceeding — the caller must
    /// never guess an order across a cyclic FK graph (R6).
    fn continue_from_configure(&mut self, cx: &mut Context<Self>) {
        let Some(source_connection) = self.resolve_source_connection(cx) else {
            self.error = Some("No active connection for the source profile".to_string());
            cx.notify();
            return;
        };
        let source_database = self
            .source_database
            .clone()
            .or_else(|| source_connection.active_database())
            .unwrap_or_default();

        let table_refs: Vec<TableRef> = self
            .rows
            .iter()
            .map(|row| row.config.source_table.clone())
            .collect();

        let mut schemas: Vec<Option<String>> =
            table_refs.iter().map(|t| t.schema.clone()).collect();
        schemas.sort();
        schemas.dedup();

        let mut fks: Vec<SchemaForeignKeyInfo> = Vec::new();
        for schema in schemas {
            match source_connection.schema_foreign_keys(&source_database, schema.as_deref()) {
                Ok(batch) => fks.extend(batch),
                Err(e) => {
                    self.error = Some(format!("Could not read foreign keys: {e}"));
                    cx.notify();
                    return;
                }
            }
        }

        match topological_order(&table_refs, &fks) {
            dbflux_core::OrderResult::Ordered(order) => {
                self.final_order = Some(order);
                self.advance_past_ordering(cx);
            }
            dbflux_core::OrderResult::Cyclic {
                ordered_prefix,
                cycle,
            } => {
                self.cyclic_prefix = ordered_prefix;
                self.reorder_list = cycle;
                self.step = WizardStep::ReorderCycle;
                cx.notify();
            }
        }
    }

    fn move_reorder_row(&mut self, index: usize, delta: isize, cx: &mut Context<Self>) {
        let Some(new_index) = index.checked_add_signed(delta) else {
            return;
        };
        if new_index >= self.reorder_list.len() {
            return;
        }
        self.reorder_list.swap(index, new_index);
        cx.notify();
    }

    fn continue_from_reorder(&mut self, cx: &mut Context<Self>) {
        let mut order = self.cyclic_prefix.clone();
        order.extend(self.reorder_list.clone());
        self.final_order = Some(order);
        self.advance_past_ordering(cx);
    }

    fn advance_past_ordering(&mut self, cx: &mut Context<Self>) {
        let has_destructive = self.rows.iter().any(|row| row.config.is_destructive());
        self.step = if has_destructive {
            WizardStep::Confirm
        } else {
            self.start_migration(cx);
            WizardStep::Running
        };
        cx.notify();
    }

    fn confirm_destructive_and_run(&mut self, cx: &mut Context<Self>) {
        self.confirmed_destructive = true;
        self.start_migration(cx);
        self.step = WizardStep::Running;
        cx.notify();
    }

    fn start_migration(&mut self, cx: &mut Context<Self>) {
        let Some(target_profile_id) = self.target_profile_id else {
            return;
        };
        let Some(source_connection) = self.resolve_source_connection(cx) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    "No active connection for this migration",
                ),
                cx,
            );
            return;
        };
        let Some(target_connection) = self.resolve_target_connection(target_profile_id, cx) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    "No active connection for this migration",
                ),
                cx,
            );
            return;
        };
        let source_database = self
            .source_database
            .clone()
            .or_else(|| source_connection.active_database())
            .unwrap_or_default();
        let target_database = target_connection.active_database().unwrap_or_default();
        let manual_order = self.final_order.clone();

        let plans: Vec<MigrationTablePlan> = self
            .rows
            .iter()
            .map(|row| MigrationTablePlan {
                source_table: row.config.source_table.clone(),
                source_columns: row.config.source_columns.clone(),
                target_schema: row.config.target_schema.clone(),
                target_table: row.config.target_table.clone(),
                mapping_mode: row.config.mapping_mode,
                column_overrides: Some(row.config.to_overrides()),
                estimated_total: None,
            })
            .collect();
        let destructive_confirmed = self.confirmed_destructive;
        let disable_referential_integrity = self.disable_ri && self.supports_disable_ri;

        self.running = true;
        self.result_summary = None;
        self.result_warnings.clear();
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = (0, None);

        let description = format!("Migrate {} table(s)", plans.len());
        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let pair = state.start_task_for_target(
                TaskKind::Migrate,
                description,
                Some(TaskTarget {
                    profile_id: target_profile_id,
                    database: Some(target_database.clone()),
                }),
            );
            cx.emit(AppStateChanged);
            pair
        });
        self.active_task_id = Some(task_id);

        let app_state = self.app_state.clone();
        let progress = Arc::clone(&self.progress);
        let ticker_progress = Arc::clone(&self.progress);
        let ticker_app_state = app_state.clone();

        cx.spawn(async move |_this, cx| {
            loop {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(150))
                    .await;

                let still_running = cx
                    .update(|cx| {
                        ticker_app_state.update(cx, |state, cx| {
                            let Some(snapshot) = state.tasks().get(task_id) else {
                                return false;
                            };
                            if snapshot.status != TaskStatus::Running {
                                return false;
                            }

                            let (rows_done, estimated_total) = *ticker_progress
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            if let Some(total) = estimated_total
                                && total > 0
                            {
                                let fraction = (rows_done as f32 / total as f32).clamp(0.0, 1.0);
                                state.tasks_mut().update_progress(task_id, fraction);
                                cx.notify();
                            }

                            true
                        })
                    })
                    .unwrap_or(false);

                if !still_running {
                    break;
                }
            }
        })
        .detach();

        cx.spawn(async move |this, cx| {
            let migration_result = cx
                .background_executor()
                .spawn(async move {
                    let options = MigrationOptions {
                        segment_size: 500,
                        source_database,
                        target_database,
                        destructive_confirmed,
                        disable_referential_integrity,
                        manual_order,
                    };

                    run_migration(
                        &source_connection,
                        &target_connection,
                        &plans,
                        &options,
                        &cancel_token,
                        move |_index, rows_done, estimated_total| {
                            if let Ok(mut guard) = progress.lock() {
                                *guard = (rows_done, estimated_total);
                            }
                        },
                    )
                })
                .await;

            this.update(cx, |this, cx| {
                this.running = false;
                this.step = WizardStep::Done;

                match migration_result {
                    Ok(MigrationOutcome::Completed(outcome)) if outcome.cancelled => {
                        app_state.update(cx, |state, cx| {
                            state.tasks_mut().cancel(task_id);
                            cx.emit(AppStateChanged);
                        });
                        this.result_summary = Some("Migration cancelled".to_string());
                    }
                    Ok(MigrationOutcome::Completed(outcome)) => {
                        let failed_table = outcome.tables.iter().find_map(|t| match &t.status {
                            TableTransferStatus::Failed { error } => {
                                Some((t.source_table.clone(), error.clone()))
                            }
                            _ => None,
                        });

                        if let Some((table, error)) = &failed_table {
                            app_state.update(cx, |state, cx| {
                                state.fail_task(task_id, format!("{table}: {error}"));
                                cx.emit(AppStateChanged);
                            });
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::Driver,
                                    format!("Migration failed on table '{table}': {error}"),
                                ),
                                cx,
                            );
                        } else {
                            app_state.update(cx, |state, cx| {
                                state.complete_task(task_id);
                                cx.emit(AppStateChanged);
                            });
                            Toast::success("Migration completed").push(cx);
                        }

                        this.result_summary = Some(Self::summarize(&outcome));
                        this.result_warnings =
                            Self::itemized_status_lines(&outcome.tables, &outcome.warnings);
                    }
                    Ok(MigrationOutcome::CyclicOrderRequired { .. }) => {
                        // The wizard always resolves ordering (auto or
                        // manual) before calling `run_migration`, so this
                        // branch is unreachable in practice; treat it the
                        // same as any other failure to run rather than
                        // panicking on an engine invariant.
                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, "FK order became cyclic mid-run".to_string());
                            cx.emit(AppStateChanged);
                        });
                        this.result_summary =
                            Some("Migration failed: FK order became cyclic mid-run".to_string());
                    }
                    Err(e) => {
                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, e.to_string());
                            cx.emit(AppStateChanged);
                        });
                        report_error(
                            UserFacingError::new(
                                ErrorKind::Driver,
                                format!("Migration failed: {e}"),
                            ),
                            cx,
                        );
                        this.result_summary = Some(format!("Migration failed: {e}"));
                    }
                }

                cx.notify();
            })
            .ok();
        })
        .detach();
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
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let close_entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            close_entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let mut frame = ModalFrame::new("migrate-wizard", &self.focus_handle, close)
            .title("Migrate Data")
            .icon(AppIcon::ArrowUpDown)
            .width(px(720.0))
            .max_height(px(640.0));

        let body = match self.step {
            WizardStep::PickTarget => self.render_pick_target(cx),
            WizardStep::Configure => self.render_configure(cx),
            WizardStep::ReorderCycle => self.render_reorder_cycle(cx),
            WizardStep::Confirm => self.render_confirm(cx),
            WizardStep::Running => self.render_running(),
            WizardStep::Done => self.render_done(cx),
        };

        frame = frame.child(body);
        frame.render(cx).into_any_element()
    }
}

impl MigrateWizard {
    fn render_pick_target(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(format!(
                "Choose a connected, compatible target for {} table(s).",
                self.source_tables.len()
            )))
            .when_some(self.error.clone(), |d, error| d.child(Text::caption(error)))
            .child(self.target_dropdown.clone())
            .child(
                Button::new(
                    "migrate-wizard-continue-target",
                    if self.loading {
                        "Loading..."
                    } else {
                        "Continue"
                    },
                )
                .disabled(self.loading)
                .on_click(cx.listener(|this, _, _, cx| this.continue_from_pick_target(cx))),
            )
            .into_any_element()
    }

    fn render_configure(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows = self.rows.iter().enumerate().map(|(table_index, row)| {
            let unmatched = row.config.unmatched_source_names();

            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .p(Spacing::SM)
                .border_1()
                .child(Text::body(format!(
                    "{} → {}",
                    row.config.source_table.qualified_name(),
                    row.config.target_table
                )))
                .child(
                    div()
                        .flex()
                        .gap(Spacing::SM)
                        .child(row.mapping_mode_dropdown.clone())
                        .child(row.rebind_target_dropdown.clone())
                        .child(row.rebind_source_dropdown.clone())
                        .child(
                            Button::new(
                                SharedString::from(format!("migrate-apply-mapping-{table_index}")),
                                "Apply Mapping",
                            )
                            .ghost()
                            .on_click(cx.listener(
                                move |this, _, _, cx| {
                                    this.apply_rebind(table_index, cx);
                                },
                            )),
                        ),
                )
                .when(!unmatched.is_empty(), |d| {
                    d.child(Text::caption(format!(
                        "Unmatched source column(s), will be skipped unless remapped: {}",
                        unmatched.join(", ")
                    )))
                })
                .into_any_element()
        });

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .children(rows)
            .when(self.supports_disable_ri, |d| {
                d.child(
                    Checkbox::new("migrate-disable-ri")
                        .checked(self.disable_ri)
                        .label("Disable referential integrity during migration")
                        .on_click(cx.listener(|this, checked: &bool, _, cx| {
                            this.disable_ri = *checked;
                            cx.notify();
                        })),
                )
            })
            .when_some(self.error.clone(), |d, error| d.child(Text::caption(error)))
            .child(
                Button::new("migrate-wizard-continue-configure", "Continue")
                    .on_click(cx.listener(|this, _, _, cx| this.continue_from_configure(cx))),
            )
            .into_any_element()
    }

    fn render_reorder_cycle(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows = self.reorder_list.iter().enumerate().map(|(index, table)| {
            div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .p(Spacing::SM)
                .border_1()
                .child(Text::body(table.qualified_name()))
                .child(
                    Button::new(
                        SharedString::from(format!("migrate-reorder-up-{index}")),
                        "Up",
                    )
                    .ghost()
                    .disabled(index == 0)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.move_reorder_row(index, -1, cx);
                    })),
                )
                .child(
                    Button::new(
                        SharedString::from(format!("migrate-reorder-down-{index}")),
                        "Down",
                    )
                    .ghost()
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.move_reorder_row(index, 1, cx);
                    })),
                )
                .into_any_element()
        });

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(
                "These tables have a circular foreign-key relationship and could not be \
                 ordered automatically. Choose a manual load order:",
            ))
            .children(rows)
            .child(
                Button::new("migrate-wizard-continue-reorder", "Continue")
                    .on_click(cx.listener(|this, _, _, cx| this.continue_from_reorder(cx))),
            )
            .into_any_element()
    }

    fn render_confirm(&self, cx: &mut Context<Self>) -> AnyElement {
        let destructive_tables: Vec<String> = self
            .rows
            .iter()
            .filter(|row| row.config.is_destructive())
            .map(|row| row.config.target_table.clone())
            .collect();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(format!(
                "This will drop-and-recreate or truncate the following table(s) before loading data: {}",
                destructive_tables.join(", ")
            )))
            .child(Text::caption("This cannot be undone. Confirm to proceed."))
            .child(
                div()
                    .flex()
                    .gap(Spacing::SM)
                    .child(
                        Button::new("migrate-wizard-cancel-confirm", "Back")
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.step = WizardStep::Configure;
                                this.confirmed_destructive = false;
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("migrate-wizard-confirm-destructive", "Yes, proceed").on_click(
                            cx.listener(|this, _, _, cx| this.confirm_destructive_and_run(cx)),
                        ),
                    ),
            )
            .into_any_element()
    }

    fn render_running(&self) -> AnyElement {
        let (rows_done, estimated_total) = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let label = match estimated_total {
            Some(total) if total > 0 => format!("{rows_done} / {total} rows"),
            _ => format!("{rows_done} rows"),
        };

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body("Migrating..."))
            .child(Text::caption(label))
            .into_any_element()
    }

    fn render_done(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .when_some(self.result_summary.clone(), |d, summary| {
                d.child(Text::body(summary))
            })
            .when(!self.result_warnings.is_empty(), |d| {
                d.child(Text::caption(self.result_warnings.join("; ")))
            })
            .child(
                Button::new("migrate-wizard-close", "Close")
                    .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
            )
            .into_any_element()
    }
}
