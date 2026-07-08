//! Sidebar entry point for the Migrate action (T28, R8): resolves the
//! sidebar's current table selection into `TableRef`s and emits
//! `SidebarEvent::RequestMigrateWizard` so the workspace opens the Migrate
//! wizard (`dbflux_ui_document::migrate_wizard`) pre-populated with them.
//! Unlike Export, this action does no I/O itself — the wizard owns picking a
//! target connection, resolving column mappings, and running the migration.

use crate::*;
use dbflux_core::TransferFamily;

/// One table selected in the sidebar for migration, resolved from a
/// `SchemaNodeId::Table` node before the wizard opens.
struct SelectedTable {
    profile_id: Uuid,
    database: Option<String>,
    table: TableRef,
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
            table: TableRef {
                schema: Some(schema),
                name,
            },
        }),
        _ => None,
    }
}

/// Result of resolving a Migrate action's table selection: same
/// anchor-profile/database filtering as Export's `select_export_tables`, so
/// a table from a different profile or database is dropped and counted
/// rather than silently vanishing from the migration.
struct SelectedTablesResolution {
    tables: Vec<SelectedTable>,
    skipped_other_profile_or_database: usize,
}

/// Resolves the tables a Migrate action rooted at `item_id` should cover: the
/// active multi-selection when `item_id` is part of it, otherwise just
/// `item_id` itself (mirrors `select_export_tables`). Resolves to an empty
/// selection when `item_id` is not itself a table.
fn select_migrate_tables(
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
    fn resolve_migrate_table_selection(&self, item_id: &str) -> SelectedTablesResolution {
        select_migrate_tables(item_id, self.active_selection())
    }

    /// Number of tables a Migrate action rooted at `item_id` would cover —
    /// used to relabel the context-menu entry, mirroring
    /// `export_table_selection_count`.
    pub(crate) fn migrate_table_selection_count(&self, item_id: &str) -> usize {
        self.resolve_migrate_table_selection(item_id).tables.len()
    }

    pub(crate) fn migrate_selected_tables(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let resolution = self.resolve_migrate_table_selection(item_id);
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

        let table_refs: Vec<TableRef> = tables.into_iter().map(|t| t.table).collect();

        cx.emit(SidebarEvent::RequestMigrateWizard {
            profile_id,
            database,
            tables: table_refs,
        });
    }
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use crate::*`/`use super::*`, which
    // pull in `gpui::*` and trigger macro recursion (see task_runner.rs).
    use super::{SelectedTable, select_migrate_tables};
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

        let resolved = select_migrate_tables(&item_id, &selection);

        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].table.name, "users");
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

        let resolved = select_migrate_tables(&users, &selection);

        let mut names: Vec<&str> = resolved
            .tables
            .iter()
            .map(|t| t.table.name.as_str())
            .collect();
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

        let resolved = select_migrate_tables(&anchor, &selection);

        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].table.name, "users");
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

        let resolved = select_migrate_tables(&anchor, &selection);

        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].table.name, "users");
        assert_eq!(resolved.skipped_other_profile_or_database, 1);
    }

    #[test]
    fn non_table_ids_in_the_selection_are_ignored_and_not_counted_as_skipped() {
        let profile_id = Uuid::new_v4();
        let anchor = table_id(profile_id, None, "public", "users");
        let profile_node_id = SchemaNodeId::Profile { profile_id }.to_string();
        let selection: HashSet<String> = [anchor.clone(), profile_node_id].into_iter().collect();

        let resolved = select_migrate_tables(&anchor, &selection);

        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].table.name, "users");
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

        let resolved = select_migrate_tables(&profile_node_id, &selection);

        assert!(resolved.tables.is_empty());
        assert_eq!(resolved.skipped_other_profile_or_database, 0);
    }
}
