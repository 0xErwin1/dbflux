/// Canonical DDL helper for the `aud_audit_events` table.
///
/// This module is the single source of truth for the `aud_audit_events` schema.
/// Migration 001 calls `create_aud_audit_events` with `with_profile_fk = true`
/// to get the FK constraint; the standalone `SqliteAuditStore` calls it with
/// `with_profile_fk = false` because `cfg_connection_profiles` may not exist
/// outside the full `StorageRuntime` migration context.
///
/// The helper is idempotent: `CREATE TABLE IF NOT EXISTS` is a no-op when the
/// table already exists, and the `ADD COLUMN IF NOT EXISTS` loop skips columns
/// that are already present. This makes it safe to call on pre-existing
/// standalone databases that were created with an older schema.
use rusqlite::Connection;

/// All 17 extended columns added by migration 002.
///
/// Listed here so the standalone store, migration 001, and this helper share
/// exactly the same column list without duplication.
const EXTENDED_COLUMNS: &[&str] = &[
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
];

/// Creates (or reconciles) the `aud_audit_events` table.
///
/// When `with_profile_fk` is `true`, the table includes a `FOREIGN KEY (profile_id)
/// REFERENCES cfg_connection_profiles(id) ON DELETE SET NULL` constraint (used by
/// migration 001, which runs inside the full storage runtime where that table
/// already exists). When `false`, the FK is omitted (standalone audit store path
/// where `cfg_connection_profiles` may not exist).
///
/// The function creates the table with all 27 columns in one shot, then runs an
/// idempotent `ADD COLUMN` loop for the 17 extended columns so that databases
/// created by an older version of the standalone store are brought up to date.
pub fn create_aud_audit_events(conn: &Connection, with_profile_fk: bool) -> rusqlite::Result<()> {
    let fk_clause = if with_profile_fk {
        ",\n    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE SET NULL"
    } else {
        ""
    };

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS aud_audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id TEXT NOT NULL,
    tool_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT,
    profile_id TEXT,
    classification TEXT,
    duration_ms INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    created_at_epoch_ms INTEGER NOT NULL,
    level TEXT,
    category TEXT,
    action TEXT,
    outcome TEXT,
    actor_type TEXT,
    source_id TEXT,
    summary TEXT,
    connection_id TEXT,
    database_name TEXT,
    driver_id TEXT,
    object_type TEXT,
    object_id TEXT,
    details_json TEXT,
    error_code TEXT,
    error_message TEXT,
    session_id TEXT,
    correlation_id TEXT{fk_clause}
)"
    );

    conn.execute_batch(&create_sql)?;

    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_actor
             ON aud_audit_events(actor_id, created_at DESC);
         CREATE INDEX IF NOT EXISTS idx_aud_audit_events_tool
             ON aud_audit_events(tool_id, created_at DESC);
         CREATE INDEX IF NOT EXISTS idx_aud_audit_events_profile
             ON aud_audit_events(profile_id);
         CREATE INDEX IF NOT EXISTS idx_aud_audit_events_decision
             ON aud_audit_events(decision);
         CREATE INDEX IF NOT EXISTS idx_aud_audit_events_created
             ON aud_audit_events(created_at DESC);
         CREATE INDEX IF NOT EXISTS idx_aud_audit_events_created_epoch
             ON aud_audit_events(created_at_epoch_ms DESC);",
    )?;

    add_extended_columns_if_missing(conn)?;

    Ok(())
}

/// Adds each of the 17 extended columns to `aud_audit_events` if they are
/// not already present.
///
/// This reconciles databases that were created by an older version of the
/// standalone store (which had only the 10 base columns) without requiring a
/// full DROP/CREATE cycle.
fn add_extended_columns_if_missing(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(aud_audit_events)")?;
    let existing: std::collections::HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;

    for col in EXTENDED_COLUMNS {
        if !existing.contains(*col) {
            conn.execute_batch(&format!(
                "ALTER TABLE aud_audit_events ADD COLUMN {} TEXT",
                col
            ))?;
        }
    }

    Ok(())
}
