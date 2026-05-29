//! Migration 011: Drop the `auth_profile_id` foreign key on
//! `cfg_connection_profiles`.
//!
//! AWS auth profiles are now reflected live from `~/.aws/config` rather than
//! stored in `cfg_auth_profiles`. A connection bound to a reflected profile
//! therefore holds an `auth_profile_id` that has no row in `cfg_auth_profiles`.
//! With foreign-key enforcement ON, saving such a connection violated the
//! `auth_profile_id -> cfg_auth_profiles(id)` constraint, so the binding never
//! persisted and appeared "deassigned" after a restart.
//!
//! SQLite cannot drop a single constraint in place, so the table is rebuilt
//! without that foreign key. The `proxy_profile_id` and `ssh_tunnel_profile_id`
//! foreign keys are kept — those profiles are still stored. The rebuild relies
//! on the caller running migrations with `PRAGMA foreign_keys = OFF` (see
//! `StorageRuntime::for_path`), otherwise dropping the parent table would
//! cascade-delete every connection's child rows.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "011_connection_drop_auth_profile_fk"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let map_err = |source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        };

        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='cfg_connection_profiles'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(map_err)?;

        if !table_exists {
            return Ok(());
        }

        // Idempotency: skip if the auth_profile_id foreign key is already gone.
        let auth_fk_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_foreign_key_list('cfg_connection_profiles') \
                 WHERE \"table\" = 'cfg_auth_profiles'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(map_err)?;

        if !auth_fk_exists {
            return Ok(());
        }

        // Rebuild without the auth_profile_id foreign key. Columns are listed
        // explicitly so the copy is independent of physical column order.
        tx.execute_batch(
            "CREATE TABLE cfg_connection_profiles_new (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                driver_id TEXT,
                description TEXT,
                kind TEXT,
                save_password INTEGER NOT NULL DEFAULT 0,
                access_kind TEXT,
                access_provider TEXT,
                favorite INTEGER DEFAULT 0,
                color TEXT,
                icon TEXT,
                auth_profile_id TEXT,
                proxy_profile_id TEXT,
                ssh_tunnel_profile_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (proxy_profile_id) REFERENCES cfg_proxy_profiles(id) ON DELETE SET NULL,
                FOREIGN KEY (ssh_tunnel_profile_id) REFERENCES cfg_ssh_tunnel_profiles(id) ON DELETE SET NULL
            );

            INSERT INTO cfg_connection_profiles_new (
                id, name, driver_id, description, kind, save_password, access_kind,
                access_provider, favorite, color, icon, auth_profile_id,
                proxy_profile_id, ssh_tunnel_profile_id, created_at, updated_at
            )
            SELECT
                id, name, driver_id, description, kind, save_password, access_kind,
                access_provider, favorite, color, icon, auth_profile_id,
                proxy_profile_id, ssh_tunnel_profile_id, created_at, updated_at
            FROM cfg_connection_profiles;

            DROP TABLE cfg_connection_profiles;

            ALTER TABLE cfg_connection_profiles_new RENAME TO cfg_connection_profiles;",
        )
        .map_err(map_err)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn fk_target_tables(conn: &Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT \"table\" FROM pragma_foreign_key_list('cfg_connection_profiles')")
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    fn seed_old_schema(conn: &Connection) {
        // A fresh in-memory connection has foreign_keys OFF by default, matching
        // how migrations run (StorageRuntime::for_path disables enforcement).
        conn.execute_batch(
            "CREATE TABLE cfg_proxy_profiles (id TEXT PRIMARY KEY);
             CREATE TABLE cfg_ssh_tunnel_profiles (id TEXT PRIMARY KEY);
             CREATE TABLE cfg_auth_profiles (id TEXT PRIMARY KEY);
             CREATE TABLE cfg_connection_profiles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                driver_id TEXT,
                description TEXT,
                kind TEXT,
                save_password INTEGER NOT NULL DEFAULT 0,
                access_kind TEXT,
                access_provider TEXT,
                favorite INTEGER DEFAULT 0,
                color TEXT,
                icon TEXT,
                auth_profile_id TEXT,
                proxy_profile_id TEXT,
                ssh_tunnel_profile_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (auth_profile_id) REFERENCES cfg_auth_profiles(id) ON DELETE SET NULL,
                FOREIGN KEY (proxy_profile_id) REFERENCES cfg_proxy_profiles(id) ON DELETE SET NULL,
                FOREIGN KEY (ssh_tunnel_profile_id) REFERENCES cfg_ssh_tunnel_profiles(id) ON DELETE SET NULL
             );",
        )
        .unwrap();
    }

    fn apply(conn: &Connection) {
        let tx = conn.unchecked_transaction().unwrap();
        MigrationImpl.run(&tx).unwrap();
        tx.commit().unwrap();
    }

    #[test]
    fn drops_auth_fk_keeps_others_and_preserves_rows() {
        let conn = Connection::open_in_memory().unwrap();
        // Migrations run with enforcement OFF (see StorageRuntime::for_path).
        conn.pragma_update(None, "foreign_keys", "OFF").unwrap();
        seed_old_schema(&conn);

        // Connection bound to a reflected auth profile id absent from cfg_auth_profiles.
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name, auth_profile_id) \
             VALUES ('c1', 'conn', 'reflected-uuid')",
            [],
        )
        .unwrap();

        assert!(fk_target_tables(&conn).contains(&"cfg_auth_profiles".to_string()));

        apply(&conn);

        let fks = fk_target_tables(&conn);
        assert!(
            !fks.contains(&"cfg_auth_profiles".to_string()),
            "auth_profile_id FK must be dropped"
        );
        assert!(
            fks.contains(&"cfg_proxy_profiles".to_string()),
            "proxy FK kept"
        );
        assert!(
            fks.contains(&"cfg_ssh_tunnel_profiles".to_string()),
            "ssh FK kept"
        );

        let auth: Option<String> = conn
            .query_row(
                "SELECT auth_profile_id FROM cfg_connection_profiles WHERE id='c1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            auth.as_deref(),
            Some("reflected-uuid"),
            "reflected binding must survive the rebuild"
        );
    }

    #[test]
    fn second_run_is_noop() {
        let conn = Connection::open_in_memory().unwrap();
        // Migrations run with enforcement OFF (see StorageRuntime::for_path).
        conn.pragma_update(None, "foreign_keys", "OFF").unwrap();
        seed_old_schema(&conn);

        apply(&conn);
        // Already dropped — must not error or re-rebuild.
        apply(&conn);

        assert!(!fk_target_tables(&conn).contains(&"cfg_auth_profiles".to_string()));
    }
}
