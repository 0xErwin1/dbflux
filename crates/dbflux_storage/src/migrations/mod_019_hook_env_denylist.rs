//! Migration 019: Add `env_denylist_json` column to `cfg_connection_profile_hooks`
//! and `cfg_hook_definitions`.
//!
//! Before this migration, hooks could not persist an env-denylist: the
//! `ConnectionHook::env_denylist` field was always reset to an empty vec when loading
//! from storage. The new column on each hook table stores the list as a JSON array so
//! that it survives a save/load cycle.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "019_hook_env_denylist"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        add_env_denylist_column(tx, "cfg_connection_profile_hooks")?;
        add_env_denylist_column(tx, "cfg_hook_definitions")?;
        Ok(())
    }
}

fn add_env_denylist_column(tx: &Transaction, table: &str) -> Result<(), MigrationError> {
    let table_exists: bool = tx
        .query_row(
            &format!("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}'"),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    if !table_exists {
        return Ok(());
    }

    let column_exists: bool = tx
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = 'env_denylist_json'"
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    if !column_exists {
        tx.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN env_denylist_json TEXT NOT NULL DEFAULT '[]';"
        ))
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::bootstrap::StorageRuntime;

    fn setup() -> StorageRuntime {
        StorageRuntime::in_memory().expect("in-memory storage runtime")
    }

    #[test]
    fn env_denylist_json_column_present_after_migration() {
        let runtime = setup();
        let conn = runtime.dbflux_db();
        let col_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_connection_profile_hooks') WHERE name = 'env_denylist_json'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .expect("pragma query");

        assert!(
            col_exists,
            "env_denylist_json column must exist after migration 019"
        );
    }

    #[test]
    fn connection_profile_hook_dto_env_denylist_round_trips() {
        let runtime = setup();
        let profiles_repo = runtime.connection_profiles();

        let profile_id = uuid::Uuid::new_v4().to_string();
        let conn = runtime.dbflux_db();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'test')",
            rusqlite::params![profile_id],
        )
        .expect("insert profile");

        let hooks_repo = profiles_repo.hooks();
        let mut dto =
            crate::repositories::connection_profile_hooks::ConnectionProfileHookDto::new_command(
                profile_id.clone(),
                "pre_connect".to_string(),
                0,
                "my-script.sh".to_string(),
            );
        dto.env_denylist = vec!["MY_SECRET_KEY".to_string(), "INTERNAL_TOKEN".to_string()];

        hooks_repo.insert(&dto).expect("insert hook dto");

        let loaded = hooks_repo
            .get_for_profile(&profile_id)
            .expect("get for profile");

        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].env_denylist,
            vec!["MY_SECRET_KEY".to_string(), "INTERNAL_TOKEN".to_string()],
            "env_denylist must survive a DTO insert/load round trip"
        );
    }

    #[test]
    fn connection_profile_hook_dto_missing_env_denylist_defaults_to_empty() {
        let runtime = setup();
        let conn = runtime.dbflux_db();

        let profile_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'test2')",
            rusqlite::params![profile_id],
        )
        .expect("insert profile");

        let hook_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO cfg_connection_profile_hooks (id, profile_id, phase, order_index, enabled, hook_kind, lua_log, lua_env_read, lua_conn_metadata, lua_process_run, inherit_env, execution_mode, on_failure) VALUES (?1, ?2, 'pre_connect', 0, 1, 'command', 1, 1, 1, 0, 1, 'blocking', 'disconnect')",
            rusqlite::params![hook_id, profile_id],
        )
        .expect("insert hook without env_denylist_json");

        let profiles_repo = runtime.connection_profiles();
        let hooks_repo = profiles_repo.hooks();
        let loaded = hooks_repo
            .get_for_profile(&profile_id)
            .expect("get for profile");

        assert_eq!(loaded.len(), 1);
        assert!(
            loaded[0].env_denylist.is_empty(),
            "env_denylist must default to empty when column value is '[]'"
        );
    }
}
