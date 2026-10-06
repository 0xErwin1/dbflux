//! Sidebar entry point for the Export wizard (R8): resolves the sidebar's
//! current table selection into `TableRef`s and emits
//! `SidebarEvent::RequestExportWizard` so the workspace opens the Export
//! wizard (`dbflux_ui_document::export_wizard`) pre-populated with them.
//! Unlike the pre-redesign action, this does no I/O and no longer picks a
//! format from a submenu — the wizard itself owns the folder picker, the
//! format choice, and running the export
//! (`dbflux_ui_document::export_wizard::run`). The selection resolution is
//! shared with Migrate in `table_selection.rs`.

use crate::*;

fn export_wizard_request(
    profile_id: Uuid,
    database: Option<String>,
    tables: Vec<TableRef>,
) -> SidebarEvent {
    SidebarEvent::RequestExportWizard {
        profile_id,
        database,
        tables,
    }
}

impl Sidebar {
    /// Number of tables an Export action rooted at `item_id` would cover —
    /// used to relabel the context-menu entry ("Export Table…" vs
    /// "Export N Tables…"), mirroring `migrate_table_selection_count`.
    pub(crate) fn export_table_selection_count(&self, item_id: &str) -> usize {
        self.table_selection_count(item_id)
    }

    pub(crate) fn request_export_wizard(&mut self, item_id: &str, cx: &mut Context<Self>) {
        self.request_table_wizard(item_id, export_wizard_request, cx);
    }
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use crate::*`/`use super::*`, which
    // pull in `gpui::*` and trigger macro recursion (see task_runner.rs).
    use super::export_wizard_request;
    use crate::SidebarEvent;
    use dbflux_core::TableRef;
    use uuid::Uuid;

    #[test]
    fn export_entry_point_requests_the_export_wizard_with_the_resolved_source() {
        let profile_id = Uuid::new_v4();
        let tables = vec![TableRef {
            schema: Some("public".to_string()),
            name: "users".to_string(),
        }];

        let event = export_wizard_request(profile_id, Some("app_db".to_string()), tables);

        let SidebarEvent::RequestExportWizard {
            profile_id: event_profile_id,
            database,
            tables,
        } = event
        else {
            panic!("the export entry point must emit RequestExportWizard");
        };
        assert_eq!(event_profile_id, profile_id);
        assert_eq!(database.as_deref(), Some("app_db"));
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
    }
}
