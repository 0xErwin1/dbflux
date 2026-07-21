/// Integration tests for migration 017: `qry_*` tables for the visual query builder.
///
/// Verifies that all four tables exist after running the full migration chain,
/// that the profile-scoped index is present, and that the UNIQUE constraint on
/// `(profile_id, name)` fires on duplicate insert.
use rusqlite::Connection;

use dbflux_storage::migrations::MigrationRegistry;

fn table_names(conn: &Connection) -> std::collections::HashSet<String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table'")
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
}

fn index_names(conn: &Connection) -> std::collections::HashSet<String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='index'")
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
}

fn insert_profile(conn: &Connection) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'P')",
        [&id],
    )
    .unwrap();
    id
}

#[test]
fn all_qry_tables_exist_after_migrations() {
    let conn = Connection::open_in_memory().unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    let tables = table_names(&conn);

    for expected in &[
        "qry_saved_queries",
        "qry_saved_query_columns",
        "qry_saved_query_sorts",
        "qry_saved_query_joins",
    ] {
        assert!(
            tables.contains(*expected),
            "table '{expected}' must exist after migration 017"
        );
    }
}

#[test]
fn profile_index_exists_after_migrations() {
    let conn = Connection::open_in_memory().unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    let indices = index_names(&conn);
    assert!(
        indices.contains("idx_qry_saved_queries_profile"),
        "index 'idx_qry_saved_queries_profile' must exist after migration 017"
    );
}

#[test]
fn unique_constraint_fires_on_duplicate_profile_name() {
    let conn = Connection::open_in_memory().unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    let profile_id = insert_profile(&conn);
    let now_ms: i64 = 1_000_000;

    conn.execute(
        "INSERT INTO qry_saved_queries \
         (id, profile_id, name, table_name, source_alias, projection_mode, \
          offset_value, created_at, updated_at) \
         VALUES (?1, ?2, 'My Query', 'users', 'users', 'all', 0, ?3, ?3)",
        rusqlite::params![uuid::Uuid::new_v4().to_string(), profile_id, now_ms],
    )
    .expect("first insert must succeed");

    let result = conn.execute(
        "INSERT INTO qry_saved_queries \
         (id, profile_id, name, table_name, source_alias, projection_mode, \
          offset_value, created_at, updated_at) \
         VALUES (?1, ?2, 'My Query', 'orders', 'orders', 'all', 0, ?3, ?3)",
        rusqlite::params![uuid::Uuid::new_v4().to_string(), profile_id, now_ms],
    );

    assert!(
        result.is_err(),
        "duplicate (profile_id, name) must be rejected by UNIQUE constraint"
    );
}

#[test]
fn same_name_in_different_profiles_is_allowed() {
    let conn = Connection::open_in_memory().unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
    MigrationRegistry::new().run_all(&conn).unwrap();

    let profile_a = insert_profile(&conn);
    let profile_b = insert_profile(&conn);
    let now_ms: i64 = 1_000_000;

    conn.execute(
        "INSERT INTO qry_saved_queries \
         (id, profile_id, name, table_name, source_alias, projection_mode, \
          offset_value, created_at, updated_at) \
         VALUES (?1, ?2, 'My Query', 'users', 'users', 'all', 0, ?3, ?3)",
        rusqlite::params![uuid::Uuid::new_v4().to_string(), profile_a, now_ms],
    )
    .expect("insert for profile_a must succeed");

    conn.execute(
        "INSERT INTO qry_saved_queries \
         (id, profile_id, name, table_name, source_alias, projection_mode, \
          offset_value, created_at, updated_at) \
         VALUES (?1, ?2, 'My Query', 'orders', 'orders', 'all', 0, ?3, ?3)",
        rusqlite::params![uuid::Uuid::new_v4().to_string(), profile_b, now_ms],
    )
    .expect("same name in a different profile must be allowed");
}
