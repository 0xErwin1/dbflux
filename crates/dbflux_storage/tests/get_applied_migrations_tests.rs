/// Tests for get_applied_migrations error propagation.
use rusqlite::Connection;

use dbflux_storage::migrations::MigrationRegistry;

/// `get_applied_migrations` must surface a per-row deserialization error rather
/// than silently skipping it.  We create `sys_migrations` by hand with a NULL
/// name column (no NOT NULL constraint) so that `row.get::<_, String>(0)` on
/// that row fails, which exercises the error-propagation path.
#[test]
fn get_applied_migrations_propagates_row_error() {
    let conn = Connection::open_in_memory().unwrap();

    // Create the table without NOT NULL so we can insert a NULL name.
    conn.execute_batch(
        "CREATE TABLE sys_migrations (name TEXT, applied_at TEXT NOT NULL DEFAULT (datetime('now')))",
    )
    .unwrap();
    conn.execute("INSERT INTO sys_migrations (name) VALUES (NULL)", [])
        .unwrap();

    let registry = MigrationRegistry::new();
    let result = registry.get_pending(&conn);

    assert!(
        result.is_err(),
        "get_pending must return Err when a sys_migrations row cannot be decoded, got: {:?}",
        result.ok().map(|v| v.len())
    );
}

#[test]
fn regression_clean_db_runs_all_migrations() {
    let conn = Connection::open_in_memory().unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    let registry = MigrationRegistry::new();
    let pending = registry.get_pending(&conn).unwrap();
    assert!(
        pending.is_empty(),
        "no migrations should be pending after run_all on a fresh DB"
    );
}
