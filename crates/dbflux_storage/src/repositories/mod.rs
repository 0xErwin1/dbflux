//! Repository modules for DBFlux internal storage.
//!
//! Each repository provides CRUD operations for a specific config domain.
//! Config repositories operate on `dbflux.db`.

pub mod app_meta;
pub mod audit;
pub mod audit_settings;
pub mod saved_filters;
pub mod traits;

pub mod auth_profile_fields;
pub mod auth_profiles;
pub mod connection_driver_configs;
pub mod connection_folders;
pub mod connection_profile_access_params;
pub mod connection_profile_configs;
pub mod connection_profile_governance;
pub mod connection_profile_governance_binding_policies;
pub mod connection_profile_governance_binding_roles;
pub mod connection_profile_governance_bindings;
pub mod connection_profile_hook_args;
pub mod connection_profile_hook_bindings;
pub mod connection_profile_hook_envs;
pub mod connection_profile_hooks;
pub mod connection_profile_settings;
pub mod connection_profile_value_refs;
pub mod connection_profiles;
pub mod driver_overrides;
pub mod driver_setting_values;
pub mod driver_settings;
pub mod general_settings;
pub mod governance_settings;
pub mod hook_commands;
pub mod hook_definitions;
pub mod hook_environment;
pub mod keybinding_overrides;
pub mod proxy_auth;
pub mod proxy_profiles;
pub mod service_args;
pub mod service_env;
pub mod services;
pub mod settings;
pub mod ssh_tunnel_auth;
pub mod ssh_tunnel_profiles;
pub mod update_settings;

pub mod state;

pub mod viz_dashboard_panels;
pub mod viz_dashboards;
pub mod viz_saved_chart_binding_y;
pub mod viz_saved_chart_series;
pub mod viz_saved_chart_source_metric_dimensions;
pub mod viz_saved_chart_source_metric_series;
pub mod viz_saved_charts;

pub mod qry_saved_document_queries;
pub mod qry_saved_queries;

pub mod sch_schema_snapshots;

/// Returns a `filter_map` adapter for the rows of a list query that keeps
/// readable rows and logs the ones that fail to read.
///
/// One corrupt row must not hide every other entry of the list it feeds, so
/// it is skipped, but never silently: the warning names the table. Use it
/// only for display lists that are never written back. A read whose result
/// is saved again, counted, or looked up by id must propagate the error,
/// because a skipped row there is lost on the next save or reads as absent.
pub(crate) fn readable_row<T>(table: &'static str) -> impl FnMut(rusqlite::Result<T>) -> Option<T> {
    move |row| match row {
        Ok(value) => Some(value),
        Err(error) => {
            log::warn!("Skipping an unreadable row of {table}: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::saved_filters::{SavedFilterDto, SavedFiltersRepository};
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::{Arc, Mutex, Once};

    static CAPTURED: Mutex<Vec<(log::Level, String)>> = Mutex::new(Vec::new());

    struct CapturingLogger;

    impl log::Log for CapturingLogger {
        fn enabled(&self, _metadata: &log::Metadata) -> bool {
            true
        }

        fn log(&self, record: &log::Record) {
            CAPTURED
                .lock()
                .expect("the captured log mutex should not be poisoned")
                .push((record.level(), record.args().to_string()));
        }

        fn flush(&self) {}
    }

    fn install_capturing_logger() {
        static INSTALL: Once = Once::new();
        static LOGGER: CapturingLogger = CapturingLogger;

        INSTALL.call_once(|| {
            log::set_logger(&LOGGER).expect("no other logger is installed in this test binary");
            log::set_max_level(log::LevelFilter::Trace);
        });
    }

    #[test]
    fn a_list_skips_an_unreadable_row_and_logs_its_table() {
        install_capturing_logger();

        let directory = tempfile::tempdir().expect("a temp directory");
        let conn = open_database(&directory.path().join("dbflux.db")).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrations should run");

        // A BLOB keeps its storage class in a TEXT column, so reading `name`
        // as a String fails for this row only.
        conn.execute(
            "INSERT INTO aud_saved_filters (name, filter_json) VALUES (X'00', '{}')",
            [],
        )
        .expect("the corrupt row inserts");

        let repo = SavedFiltersRepository::new(Arc::new(Mutex::new(conn)));
        repo.upsert(&SavedFilterDto {
            id: None,
            name: "Errors Only".to_string(),
            description: None,
            filter_json: "{}".to_string(),
            created_at: String::new(),
            updated_at: String::new(),
        })
        .expect("the readable row inserts");

        let filters = repo.list().expect("the list succeeds despite one bad row");
        let names: Vec<&str> = filters.iter().map(|filter| filter.name.as_str()).collect();
        assert_eq!(names, vec!["Errors Only"]);

        let captured = CAPTURED
            .lock()
            .expect("the captured log mutex should not be poisoned");
        assert!(
            captured
                .iter()
                .any(|(level, message)| *level == log::Level::Warn
                    && message.contains("aud_saved_filters")),
            "expected a warning naming aud_saved_filters, got {captured:?}"
        );
    }
}
