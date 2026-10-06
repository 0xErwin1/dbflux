//! Table-selection resolution shared by the sidebar's Export and Migrate
//! entry points (`export_tables.rs`, `migrate_tables.rs`). Both resolve the
//! sidebar's current table selection into `TableRef`s the same way and differ
//! only in the `SidebarEvent` they emit to open their wizard.

use crate::*;
use dbflux_core::TransferFamily;

/// One table selected in the sidebar for a data-transfer action, resolved
/// from a `SchemaNodeId::Table` node before the wizard opens.
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

/// Result of resolving a data-transfer action's table selection: the tables
/// that will actually be covered, plus how many candidate tables were dropped
/// because they belong to a different profile/database than the anchor —
/// surfaced to the user as a non-blocking notice rather than silently
/// vanishing from the export or migration.
struct SelectedTablesResolution {
    tables: Vec<SelectedTable>,
    skipped_other_profile_or_database: usize,
}

/// Resolves the tables an Export or Migrate action rooted at `item_id` should
/// cover: the active multi-selection when `item_id` is part of it, otherwise
/// just `item_id` itself. Only tables sharing the right-clicked table's
/// profile AND database are kept — the wizard uses one physical connection,
/// and `ConnectionPerDatabase` drivers (MySQL/MariaDB) keep a separate
/// connection per database. Resolves to an empty selection when `item_id` is
/// not itself a table.
///
/// A pure function of `(item_id, active_selection)` — no GPUI context needed
/// — so it is unit-testable without a `Sidebar` entity.
fn select_tables(item_id: &str, active_selection: &HashSet<String>) -> SelectedTablesResolution {
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
        .partition(|selected| {
            selected.profile_id == anchor.profile_id && selected.database == anchor.database
        });

    SelectedTablesResolution {
        tables,
        skipped_other_profile_or_database: skipped.len(),
    }
}

/// Builds the `SidebarEvent` that opens a data-transfer wizard for the
/// resolved source `(profile_id, database)` and its tables.
pub(super) type TableWizardRequest = fn(Uuid, Option<String>, Vec<TableRef>) -> SidebarEvent;

impl Sidebar {
    fn resolve_table_selection(&self, item_id: &str) -> SelectedTablesResolution {
        select_tables(item_id, self.active_selection())
    }

    /// Number of tables an Export or Migrate action rooted at `item_id` would
    /// cover — used to relabel the context-menu entry ("Export Table…" vs
    /// "Export N Tables…").
    pub(super) fn table_selection_count(&self, item_id: &str) -> usize {
        self.resolve_table_selection(item_id).tables.len()
    }

    /// Resolves the table selection rooted at `item_id`, warns about tables
    /// skipped for belonging to another profile/database, and emits the event
    /// built by `request` when the source connection is connected and uses
    /// the SQL transfer family.
    pub(super) fn request_table_wizard(
        &mut self,
        item_id: &str,
        request: TableWizardRequest,
        cx: &mut Context<Self>,
    ) {
        let resolution = self.resolve_table_selection(item_id);
        let tables = resolution.tables;

        if resolution.skipped_other_profile_or_database > 0 {
            dbflux_ui_base::toast::Toast::warning(crate::labels::skipped_tables_label(
                resolution.skipped_other_profile_or_database,
            ))
            .push(cx);
        }

        let Some(profile_id) = tables.first().map(|selected| selected.profile_id) else {
            return;
        };
        let database = tables
            .first()
            .and_then(|selected| selected.database.clone());

        let state = self.app_state.read(cx);
        let Some(connected) = state.connections().get(&profile_id) else {
            return;
        };
        if connected.connection.metadata().transfer_family != TransferFamily::Sql {
            return;
        }

        let table_refs: Vec<TableRef> = tables.into_iter().map(|selected| selected.table).collect();

        cx.emit(request(profile_id, database, table_refs));
    }
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use crate::*`/`use super::*`, which
    // pull in `gpui::*` and trigger macro recursion (see task_runner.rs).
    use super::{SelectedTable, select_tables};
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

        let resolved = select_tables(&item_id, &selection);

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

        let resolved = select_tables(&users, &selection);

        let mut names: Vec<&str> = resolved
            .tables
            .iter()
            .map(|selected| selected.table.name.as_str())
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

        let resolved = select_tables(&anchor, &selection);

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

        let resolved = select_tables(&anchor, &selection);

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

        let resolved = select_tables(&anchor, &selection);

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

        let resolved = select_tables(&profile_node_id, &selection);

        assert!(resolved.tables.is_empty());
        assert_eq!(resolved.skipped_other_profile_or_database, 0);
    }
}
