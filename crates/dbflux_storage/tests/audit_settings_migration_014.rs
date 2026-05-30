/// Integration tests for migration 014: `log_capture_min_level` column on
/// `cfg_audit_settings`.
///
/// These are promoted from the inline tests in the migration module so that
/// they also exercise the repository layer on top of the migrated schema.
use rusqlite::Connection;

use dbflux_storage::migrations::MigrationRegistry;
use dbflux_storage::repositories::audit_settings::AuditSettingsRepository;

fn columns(conn: &Connection, table: &str) -> std::collections::HashSet<String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
}

#[test]
fn fresh_db_has_log_capture_min_level_column_after_all_migrations() {
    let conn = Connection::open_in_memory().unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    let cols = columns(&conn, "cfg_audit_settings");
    assert!(
        cols.contains("log_capture_min_level"),
        "cfg_audit_settings must have log_capture_min_level after running all migrations"
    );
}

#[test]
fn default_value_for_log_capture_min_level_is_info() {
    let conn = Connection::open_in_memory().unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    conn.execute(
        "INSERT OR IGNORE INTO cfg_audit_settings (id) VALUES (1)",
        [],
    )
    .unwrap();

    let level: String = conn
        .query_row(
            "SELECT log_capture_min_level FROM cfg_audit_settings WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(
        level, "info",
        "log_capture_min_level default must be 'info'"
    );
}

#[test]
fn repository_reads_log_capture_min_level_from_db() {
    let conn = Connection::open_in_memory().unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    let shared = std::sync::Arc::new(conn);
    let repo = AuditSettingsRepository::new(shared);

    let defaults = dbflux_storage::repositories::audit_settings::AuditSettingsDto::default();
    repo.upsert(&defaults).unwrap();

    let settings = repo
        .get()
        .unwrap()
        .expect("settings row should exist after upsert");
    assert_eq!(
        settings.log_capture_min_level, "info",
        "repository should return 'info' as the default log_capture_min_level"
    );
}

#[test]
fn update_log_capture_min_level_persists_new_value() {
    let conn = Connection::open_in_memory().unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    let shared = std::sync::Arc::new(conn);
    let repo = AuditSettingsRepository::new(shared);

    let defaults = dbflux_storage::repositories::audit_settings::AuditSettingsDto::default();
    repo.upsert(&defaults).unwrap();

    repo.update_log_capture_min_level("warn").unwrap();

    let settings = repo.get().unwrap().expect("settings row should exist");
    assert_eq!(
        settings.log_capture_min_level, "warn",
        "log_capture_min_level should be 'warn' after update"
    );
}
