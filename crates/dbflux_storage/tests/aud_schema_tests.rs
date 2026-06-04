/// Tests for the aud_schema DDL helper (AUDIT-5).
///
/// Verifies that the aud_audit_events table created by the helper has the same
/// columns as the table created by the full migration path (001 + 002), and that
/// the helper is idempotent and safe for old standalone databases.
use rusqlite::Connection;

use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::migrations::aud_schema;

fn columns_for_table(conn: &Connection, table: &str) -> std::collections::BTreeSet<String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({})", table))
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// Columns that must be present in aud_audit_events regardless of DB path.
fn expected_columns() -> std::collections::BTreeSet<String> {
    [
        "id",
        "actor_id",
        "tool_id",
        "decision",
        "reason",
        "profile_id",
        "classification",
        "duration_ms",
        "created_at",
        "created_at_epoch_ms",
        "level",
        "category",
        "action",
        "outcome",
        "actor_type",
        "source_id",
        "summary",
        "connection_id",
        "database_name",
        "driver_id",
        "object_type",
        "object_id",
        "details_json",
        "error_code",
        "error_message",
        "session_id",
        "correlation_id",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

#[test]
fn standalone_helper_creates_all_expected_columns_without_cfg_tables() {
    let conn = Connection::open_in_memory().unwrap();

    aud_schema::create_aud_audit_events(&conn, false).expect("helper should succeed");

    let cols = columns_for_table(&conn, "aud_audit_events");
    let expected = expected_columns();

    assert_eq!(
        cols, expected,
        "standalone helper must produce exactly the expected 27 columns"
    );

    // No cfg_connection_profiles table must have been created by the helper.
    let has_cfg: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_connection_profiles'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)
        .unwrap_or(false);
    assert!(!has_cfg, "standalone helper must not create cfg_* tables");
}

#[test]
fn migration_path_produces_same_columns_as_standalone_helper() {
    // Migration path: run 001 + 002 through StorageRuntime.
    let rt = StorageRuntime::in_memory().expect("runtime should initialize");
    let migration_conn = rt.open_dbflux_db().expect("should open db");
    let migration_cols = columns_for_table(&migration_conn, "aud_audit_events");

    // Standalone helper path: fresh in-memory DB, helper only.
    let standalone_conn = Connection::open_in_memory().unwrap();
    aud_schema::create_aud_audit_events(&standalone_conn, false)
        .expect("standalone helper should succeed");
    let standalone_cols = columns_for_table(&standalone_conn, "aud_audit_events");

    assert_eq!(
        migration_cols, standalone_cols,
        "migration path and standalone helper must produce identical column sets"
    );
}

#[test]
fn standalone_helper_is_idempotent_on_second_call() {
    let conn = Connection::open_in_memory().unwrap();

    aud_schema::create_aud_audit_events(&conn, false).expect("first call should succeed");
    aud_schema::create_aud_audit_events(&conn, false).expect("second call must be idempotent");

    let cols = columns_for_table(&conn, "aud_audit_events");
    assert_eq!(cols, expected_columns());
}
