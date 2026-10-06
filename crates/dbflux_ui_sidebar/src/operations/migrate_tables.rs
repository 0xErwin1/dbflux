//! Sidebar entry point for the Migrate action (T28, R8): resolves the
//! sidebar's current table selection into `TableRef`s and emits
//! `SidebarEvent::RequestMigrateWizard` so the workspace opens the Migrate
//! wizard (`dbflux_ui_document::migrate_wizard`) pre-populated with them.
//! Unlike Export, this action does no I/O itself — the wizard owns picking a
//! target connection, resolving column mappings, and running the migration.
//! The selection resolution is shared with Export in `table_selection.rs`.

use crate::*;

fn migrate_wizard_request(
    profile_id: Uuid,
    database: Option<String>,
    tables: Vec<TableRef>,
) -> SidebarEvent {
    SidebarEvent::RequestMigrateWizard {
        profile_id,
        database,
        tables,
    }
}

impl Sidebar {
    /// Number of tables a Migrate action rooted at `item_id` would cover —
    /// used to relabel the context-menu entry, mirroring
    /// `export_table_selection_count`.
    pub(crate) fn migrate_table_selection_count(&self, item_id: &str) -> usize {
        self.table_selection_count(item_id)
    }

    pub(crate) fn migrate_selected_tables(&mut self, item_id: &str, cx: &mut Context<Self>) {
        self.request_table_wizard(item_id, migrate_wizard_request, cx);
    }
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use crate::*`/`use super::*`, which
    // pull in `gpui::*` and trigger macro recursion (see task_runner.rs).
    use super::migrate_wizard_request;
    use crate::SidebarEvent;
    use dbflux_core::TableRef;
    use uuid::Uuid;

    #[test]
    fn migrate_entry_point_requests_the_migrate_wizard_with_the_resolved_source() {
        let profile_id = Uuid::new_v4();
        let tables = vec![TableRef {
            schema: Some("public".to_string()),
            name: "users".to_string(),
        }];

        let event = migrate_wizard_request(profile_id, Some("app_db".to_string()), tables);

        let SidebarEvent::RequestMigrateWizard {
            profile_id: event_profile_id,
            database,
            tables,
        } = event
        else {
            panic!("the migrate entry point must emit RequestMigrateWizard");
        };
        assert_eq!(event_profile_id, profile_id);
        assert_eq!(database.as_deref(), Some("app_db"));
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
    }
}
