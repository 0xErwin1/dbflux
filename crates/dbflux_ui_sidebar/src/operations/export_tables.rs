//! Sidebar entry point for bulk table Export (R8): resolves the sidebar's
//! current table selection into `dbflux_transfer::export::ExportTable`s and
//! runs the export on a background thread, wiring progress and cancellation
//! into the real `TaskManager` (carry-forward guard from Batch 1's verify
//! report — `on_progress` must reach `TaskManager::update_progress`, and
//! `TransferOutcome::Cancelled` must reach the real task state).

use crate::*;
use dbflux_core::{TaskKind, TaskStatus, TaskTarget, TransferColumn, TransferFamily};
use dbflux_transfer::TableTransferStatus;
use dbflux_transfer::export::{ExportOptions, ExportTable, ExportedTable, run_export};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use std::sync::{Arc, Mutex};

/// One table selected in the sidebar for export, resolved from a
/// `SchemaNodeId::Table` node before any async/background work starts.
struct SelectedTable {
    profile_id: Uuid,
    database: Option<String>,
    schema: String,
    name: String,
}

fn table_node(item_id: &str) -> Option<SelectedTable> {
    match parse_node_id(item_id) {
        Some(SchemaNodeId::Table {
            profile_id,
            database,
            schema,
            name,
        }) => Some(SelectedTable {
            profile_id,
            database,
            schema,
            name,
        }),
        _ => None,
    }
}

/// Result of resolving an Export action's table selection: the tables that
/// will actually be exported, plus how many candidate tables were dropped
/// because they belong to a different profile/database than the anchor —
/// surfaced to the user as a non-blocking notice rather than silently
/// vanishing from the export (carry-forward WARNING from Batch 2's verify).
struct SelectedTablesResolution {
    tables: Vec<SelectedTable>,
    skipped_other_profile_or_database: usize,
}

/// Resolves the tables an Export action rooted at `item_id` should cover: the
/// active multi-selection when `item_id` is part of it, otherwise just
/// `item_id` itself (matches `batch_delete_label`'s single-vs-batch pattern).
/// Only tables sharing the right-clicked table's profile AND database are
/// kept — a single `run_export` call uses one physical connection, and
/// `ConnectionPerDatabase` drivers (MySQL/MariaDB) keep a separate connection
/// per database. Resolves to an empty selection when `item_id` is not itself
/// a table.
///
/// A pure function of `(item_id, active_selection)` — no GPUI context needed
/// — so it is unit-testable without a `Sidebar` entity.
fn select_export_tables(
    item_id: &str,
    active_selection: &HashSet<String>,
) -> SelectedTablesResolution {
    let Some(anchor) = table_node(item_id) else {
        return SelectedTablesResolution {
            tables: Vec::new(),
            skipped_other_profile_or_database: 0,
        };
    };

    let mut ids: Vec<String> = if active_selection.contains(item_id) {
        active_selection.iter().cloned().collect()
    } else {
        vec![item_id.to_string()]
    };
    ids.sort();

    let (tables, skipped): (Vec<SelectedTable>, Vec<SelectedTable>) = ids
        .iter()
        .filter_map(|id| table_node(id))
        .partition(|t| t.profile_id == anchor.profile_id && t.database == anchor.database);

    SelectedTablesResolution {
        tables,
        skipped_other_profile_or_database: skipped.len(),
    }
}

impl Sidebar {
    fn resolve_export_table_selection(&self, item_id: &str) -> SelectedTablesResolution {
        select_export_tables(item_id, self.active_selection())
    }

    /// Number of tables an Export action rooted at `item_id` would cover —
    /// used to relabel the context-menu entry ("Export Table…" vs
    /// "Export N Tables…"), mirroring `batch_delete_label`.
    pub(crate) fn export_table_selection_count(&self, item_id: &str) -> usize {
        self.resolve_export_table_selection(item_id).tables.len()
    }

    pub(crate) fn export_selected_tables(
        &mut self,
        item_id: &str,
        format: dbflux_transfer::FileFormat,
        cx: &mut Context<Self>,
    ) {
        let resolution = self.resolve_export_table_selection(item_id);
        let tables = resolution.tables;

        if resolution.skipped_other_profile_or_database > 0 {
            let count = resolution.skipped_other_profile_or_database;
            let noun = if count == 1 { "table" } else { "tables" };
            dbflux_ui_base::toast::Toast::warning(format!(
                "{count} {noun} outside the active profile/database were skipped"
            ))
            .push(cx);
        }

        let Some(profile_id) = tables.first().map(|t| t.profile_id) else {
            return;
        };
        let database = tables.first().and_then(|t| t.database.clone());

        let state = self.app_state.read(cx);
        let Some(connected) = state.connections().get(&profile_id) else {
            return;
        };
        if connected.connection.metadata().transfer_family != TransferFamily::Sql {
            return;
        }

        let connection = match &database {
            Some(db) => connected.connection_for_database(db),
            None => connected.connection.clone(),
        };
        let driver_id = connected.connection.metadata().id.clone();
        let profile_name = connected.profile.name.clone();

        let table_specs: Vec<(Option<String>, String)> = tables
            .into_iter()
            .map(|t| (Some(t.schema), t.name))
            .collect();
        let dialog_available = dbflux_ui_base::file_dialog::is_native_file_dialog_available();
        let app_state = self.app_state.clone();

        cx.spawn(async move |_this, cx| {
            let base_dir = if dialog_available {
                match rfd::AsyncFileDialog::new()
                    .set_title("Choose Export Folder")
                    .pick_folder()
                    .await
                {
                    Some(handle) => handle.path().to_path_buf(),
                    None => return, // user cancelled — no toast, no audit.
                }
            } else {
                match dbflux_ui_base::file_dialog::fallback_export_dir() {
                    Ok(dir) => dir,
                    Err(err) => {
                        cx.update(|cx| {
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::Storage,
                                    format!(
                                        "Export failed — no folder picker available and the \
                                         fallback export directory could not be created: {err}"
                                    ),
                                ),
                                cx,
                            );
                        })
                        .ok();
                        return;
                    }
                }
            };

            let output_dir = base_dir.join(format!(
                "dbflux-export-{}",
                dbflux_core::chrono::Utc::now().format("%Y%m%d-%H%M%S%.3f")
            ));

            let description = format!("Export {} table(s) from {profile_name}", table_specs.len());

            let Ok((task_id, cancel_token)) = cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    let pair = state.start_task_for_target(
                        TaskKind::Export,
                        description,
                        Some(TaskTarget {
                            profile_id,
                            database: database.clone(),
                        }),
                    );
                    cx.emit(AppStateChanged);
                    pair
                })
            }) else {
                return;
            };

            let mut export_tables = Vec::with_capacity(table_specs.len());
            let mut resolve_error = None;

            for (schema, name) in &table_specs {
                let effective_db = database
                    .clone()
                    .unwrap_or_else(|| schema.clone().unwrap_or_default());
                match connection.table_details(&effective_db, schema.as_deref(), name) {
                    Ok(info) => {
                        let columns: Vec<TransferColumn> = info
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

                        export_tables.push(ExportTable {
                            schema: schema.clone(),
                            name: name.clone(),
                            columns,
                            // `TableSource` auto-counts via `SELECT COUNT(*)`
                            // when this is `None` (falling back to
                            // indeterminate progress if that query fails),
                            // so the export path's progress fraction is real
                            // rather than permanently stuck at 0.
                            estimated_total: None,
                        });
                    }
                    Err(e) => {
                        let table_label = match schema {
                            Some(schema) => format!("{schema}.{name}"),
                            None => name.clone(),
                        };
                        resolve_error = Some(format!("{table_label}: {e}"));
                        break;
                    }
                }
            }

            if let Some(err) = resolve_error {
                cx.update(|cx| {
                    app_state.update(cx, |state, cx| {
                        state.fail_task(task_id, err.clone());
                        cx.emit(AppStateChanged);
                    });
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Driver,
                            format!("Export failed while reading table schema: {err}"),
                        ),
                        cx,
                    );
                })
                .ok();
                return;
            }

            // Ticker: polls a shared progress cell every 150ms and pushes it
            // into the real TaskManager. Self-terminates once the task is no
            // longer Running (covers success, failure, and cancellation
            // uniformly without needing a second signal).
            let progress: Arc<Mutex<(u64, Option<u64>)>> = Arc::new(Mutex::new((0, None)));
            let ticker_progress = Arc::clone(&progress);
            let ticker_app_state = app_state.clone();
            cx.spawn(async move |cx| {
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
                                    let fraction =
                                        (rows_done as f32 / total as f32).clamp(0.0, 1.0);
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

            let database_for_options = database.clone().unwrap_or_default();

            let export_result = cx
                .background_executor()
                .spawn({
                    let connection = connection.clone();
                    let cancel_token = cancel_token.clone();
                    let progress = Arc::clone(&progress);
                    let output_dir = output_dir.clone();
                    async move {
                        let options = ExportOptions {
                            driver_id: &driver_id,
                            database: &database_for_options,
                            format,
                            segment_size: 500,
                        };

                        run_export(
                            &connection,
                            &export_tables,
                            &output_dir,
                            &options,
                            &cancel_token,
                            move |_table_index, rows_done, estimated_total| {
                                if let Ok(mut guard) = progress.lock() {
                                    *guard = (rows_done, estimated_total);
                                }
                            },
                        )
                    }
                })
                .await;

            cx.update(|cx| match export_result {
                Ok(outcome) if outcome.cancelled => {
                    app_state.update(cx, |state, cx| {
                        state.tasks_mut().cancel(task_id);
                        cx.emit(AppStateChanged);
                    });
                }
                Ok(outcome) => {
                    let failed_table = outcome.tables.iter().find_map(|t| match &t.status {
                        TableTransferStatus::Failed { error } => {
                            Some((t.name.clone(), error.clone()))
                        }
                        _ => None,
                    });

                    if let Some((table, error)) = &failed_table {
                        app_state.update(cx, |state, cx| {
                            state.fail_task_with_details(
                                task_id,
                                format!("{table}: {error}"),
                                itemized_table_details(&outcome.tables),
                            );
                            cx.emit(AppStateChanged);
                        });
                        report_error(
                            UserFacingError::new(
                                ErrorKind::Driver,
                                format!("Export failed on table '{table}': {error}"),
                            ),
                            cx,
                        );
                    } else {
                        let completed = outcome
                            .tables
                            .iter()
                            .filter(|t| matches!(t.status, TableTransferStatus::Completed { .. }))
                            .count();
                        app_state.update(cx, |state, cx| {
                            state.complete_task(task_id);
                            cx.emit(AppStateChanged);
                        });
                        dbflux_ui_base::toast::Toast::success(format!(
                            "Exported {completed} table(s) to {}",
                            output_dir.display()
                        ))
                        .push(cx);
                    }
                }
                Err(e) => {
                    app_state.update(cx, |state, cx| {
                        state.fail_task(task_id, e.to_string());
                        cx.emit(AppStateChanged);
                    });
                    report_error(
                        UserFacingError::new(ErrorKind::Driver, format!("Export failed: {e}")),
                        cx,
                    );
                }
            })
            .ok();
        })
        .detach();
    }
}

/// Renders one status line per planned table for the Tasks panel's expandable
/// details, so a mid-run failure (R4-002/B-007) shows exactly which tables
/// succeeded, which one failed with what error, and which were never
/// attempted — instead of only the last error in a single toast.
fn itemized_table_details(tables: &[ExportedTable]) -> String {
    tables
        .iter()
        .map(|t| {
            let label = match &t.schema {
                Some(schema) => format!("{schema}.{}", t.name),
                None => t.name.clone(),
            };
            match &t.status {
                TableTransferStatus::Completed { rows } => {
                    format!("{label}: completed ({rows} row(s))")
                }
                TableTransferStatus::Skipped => format!("{label}: skipped"),
                TableTransferStatus::Failed { error } => format!("{label}: FAILED — {error}"),
                TableTransferStatus::NotStarted => format!("{label}: not attempted"),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use crate::*`/`use super::*`, which
    // pull in `gpui::*` and trigger macro recursion (see task_runner.rs).
    use super::{SelectedTable, select_export_tables};
    use dbflux_core::SchemaNodeId;
    use std::collections::HashSet;
    use uuid::Uuid;

    fn table_id(profile_id: Uuid, database: Option<&str>, schema: &str, name: &str) -> String {
        SchemaNodeId::Table {
            profile_id,
            database: database.map(str::to_string),
            schema: schema.to_string(),
            name: name.to_string(),
        }
        .to_string()
    }

    fn profile_id_of(table: &SelectedTable) -> Uuid {
        table.profile_id
    }

    #[test]
    fn single_right_clicked_table_not_in_any_selection_resolves_to_itself() {
        let profile_id = Uuid::new_v4();
        let item_id = table_id(profile_id, None, "public", "users");
        let selection: HashSet<String> = HashSet::new();

        let resolved = select_export_tables(&item_id, &selection);

        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].name, "users");
        assert_eq!(resolved.skipped_other_profile_or_database, 0);
    }

    #[test]
    fn right_clicked_table_in_a_multi_selection_resolves_to_the_whole_selection() {
        let profile_id = Uuid::new_v4();
        let users = table_id(profile_id, None, "public", "users");
        let orders = table_id(profile_id, None, "public", "orders");
        let items = table_id(profile_id, None, "public", "items");
        let selection: HashSet<String> = [users.clone(), orders.clone(), items.clone()]
            .into_iter()
            .collect();

        let resolved = select_export_tables(&users, &selection);

        let mut names: Vec<&str> = resolved.tables.iter().map(|t| t.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["items", "orders", "users"]);
        assert_eq!(resolved.skipped_other_profile_or_database, 0);
    }

    #[test]
    fn tables_from_a_different_profile_are_excluded_and_counted_as_skipped() {
        let profile_a = Uuid::new_v4();
        let profile_b = Uuid::new_v4();
        let anchor = table_id(profile_a, None, "public", "users");
        let other_profile_table = table_id(profile_b, None, "public", "orders");
        let selection: HashSet<String> =
            [anchor.clone(), other_profile_table].into_iter().collect();

        let resolved = select_export_tables(&anchor, &selection);

        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].name, "users");
        assert_eq!(profile_id_of(&resolved.tables[0]), profile_a);
        assert_eq!(
            resolved.skipped_other_profile_or_database, 1,
            "the other-profile table must be reported as skipped, not silently dropped"
        );
    }

    #[test]
    fn tables_from_a_different_database_are_excluded_and_counted_as_skipped() {
        let profile_id = Uuid::new_v4();
        let anchor = table_id(profile_id, Some("app_db"), "public", "users");
        let other_db_table = table_id(profile_id, Some("other_db"), "public", "orders");
        let selection: HashSet<String> = [anchor.clone(), other_db_table].into_iter().collect();

        let resolved = select_export_tables(&anchor, &selection);

        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].name, "users");
        assert_eq!(resolved.skipped_other_profile_or_database, 1);
    }

    #[test]
    fn non_table_ids_in_the_selection_are_ignored_and_not_counted_as_skipped() {
        let profile_id = Uuid::new_v4();
        let anchor = table_id(profile_id, None, "public", "users");
        let profile_node_id = SchemaNodeId::Profile { profile_id }.to_string();
        let selection: HashSet<String> = [anchor.clone(), profile_node_id].into_iter().collect();

        let resolved = select_export_tables(&anchor, &selection);

        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].name, "users");
        assert_eq!(
            resolved.skipped_other_profile_or_database, 0,
            "a non-table selection member is not a data-transfer skip"
        );
    }

    #[test]
    fn non_table_anchor_resolves_to_an_empty_selection() {
        let profile_id = Uuid::new_v4();
        let profile_node_id = SchemaNodeId::Profile { profile_id }.to_string();
        let selection: HashSet<String> = HashSet::new();

        let resolved = select_export_tables(&profile_node_id, &selection);

        assert!(resolved.tables.is_empty());
        assert_eq!(resolved.skipped_other_profile_or_database, 0);
    }
}
