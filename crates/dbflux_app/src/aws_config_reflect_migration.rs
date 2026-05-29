//! One-time migration: rebind stored AWS auth-profile rows to deterministic UUIDs.
//!
//! ## What this migration does
//!
//! Before `aws-config-reflect`, DBFlux stored AWS auth profiles as opaque rows
//! in `cfg_auth_profiles` with random UUIDs. After the change, AWS profiles are
//! reflected live from `~/.aws/config` and `~/.aws/credentials`, with identity
//! determined by `UUIDv5(AWS_AUTH_NAMESPACE, provider_id + ":" + name)`.
//!
//! This migration bridges the gap for existing installations:
//!
//! 1. For each stored AWS auth-profile row, look for a matching section name in
//!    the AWS config files.
//! 2. If found: compute the deterministic UUID, rebind every
//!    `cfg_connection_profiles.auth_profile_id` that referenced the old random
//!    UUID, then delete the now-redundant stored row — all in one transaction.
//! 3. If not found: mark the stored row as dangling (no rebind, row kept),
//!    surface a warning.
//!
//! ## Provider-id mapping
//!
//! Stored rows that carried `provider_id = "aws-static-credentials"` are
//! considered candidates for the reflected `aws-shared-credentials` provider,
//! because that provider replaced the old static-credentials concept. Their new
//! deterministic UUID is computed with `provider_id = "aws-shared-credentials"`.
//!
//! | Stored provider_id          | Reflected provider_id       |
//! |-----------------------------|------------------------------|
//! | `aws-sso`                   | `aws-sso`                    |
//! | `aws-sso-session`           | `aws-sso-session`            |
//! | `aws-shared-credentials`    | `aws-shared-credentials`     |
//! | `aws-static-credentials`    | `aws-shared-credentials`     |
//!
//! ## Keyring secret handling
//!
//! Keyring secrets are **never deleted** by this migration. After a successful
//! rebind, the old keyring entry becomes orphaned (the reflected provider reads
//! credentials from `~/.aws/credentials` directly). The orphaned entry is logged
//! at info level. A consent-gated cleanup is future work and is NOT part of this
//! migration.
//!
//! ## Dangling profiles
//!
//! - **keyring-only**: a stored `aws-static-credentials` row has a keyring
//!   secret but no matching section in either config file. The row is marked
//!   `dangling_origin = "keyring-only"`. Secret untouched. Row kept. Connection
//!   bindings unchanged.
//! - **file-gone**: any stored AWS row (non-static provider) whose name does not
//!   appear in the config files. Marked `dangling_origin = "file-gone"`. Row
//!   kept. Connection bindings unchanged.
//!
//! ## Idempotency
//!
//! The `sys_app_meta` key `aws_config_reflect_migrated` acts as a guard. Once
//! set, subsequent calls to `run_aws_config_reflect_migration` are no-ops.

use std::collections::{HashMap, HashSet};

use log::{info, warn};

use dbflux_core::auth::aws_profile_uuid;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::error::StorageError;

/// Name of the idempotency marker stored in `sys_app_meta`.
pub const MIGRATION_MARKER_KEY: &str = "aws_config_reflect_migrated";

/// Provider IDs that are considered AWS-owned and are candidates for migration.
const AWS_PROVIDER_IDS: &[&str] = &[
    "aws-sso",
    "aws-sso-session",
    "aws-shared-credentials",
    "aws-static-credentials",
];

/// Maps a stored provider_id to the reflected provider_id used to compute the
/// deterministic UUID. The old `aws-static-credentials` concept was folded into
/// `aws-shared-credentials` when DBFlux stopped owning AWS secrets.
fn reflected_provider_id(stored_provider_id: &str) -> &str {
    match stored_provider_id {
        "aws-static-credentials" => "aws-shared-credentials",
        other => other,
    }
}

/// Outcome for a single stored AWS auth-profile row during migration.
#[derive(Debug, PartialEq)]
pub enum ProfileMigrationOutcome {
    /// The profile was matched in the config files, rebind was committed, and
    /// the stored row was deleted.
    Rebound {
        old_id: String,
        new_id: String,
        connections_rebound: usize,
    },
    /// The profile name was not found in the config files. The row is kept and
    /// marked dangling. The `origin` field describes why it is dangling.
    Dangling { id: String, origin: DanglingOrigin },
}

/// Describes why a profile is dangling.
#[derive(Debug, PartialEq)]
pub enum DanglingOrigin {
    /// The profile had a keyring secret but no matching file entry exists.
    KeyringOnly,
    /// The profile name no longer appears in the config files.
    FileGone,
}

impl DanglingOrigin {
    pub fn as_str(&self) -> &'static str {
        match self {
            DanglingOrigin::KeyringOnly => "keyring-only",
            DanglingOrigin::FileGone => "file-gone",
        }
    }
}

/// Summary of the entire migration run.
#[derive(Debug, Default)]
pub struct MigrationSummary {
    pub rebound: Vec<ProfileMigrationOutcome>,
    pub dangling: Vec<ProfileMigrationOutcome>,
}

/// Runs the one-time AWS config reflection migration.
///
/// Returns a summary of what was done. If the idempotency marker is already set,
/// returns an empty summary immediately.
///
/// # Parameters
///
/// - `runtime`: open storage runtime with migration 010 applied.
/// - `config_section_names`: section names present in `~/.aws/config`
///   (non-SSO: from `shared_profile_names()`; SSO: from `config_profile_names()`).
///   For a full match, pass ALL profile names from both files unioned.
/// - `credentials_names`: section names from `~/.aws/credentials` (bare `[NAME]`).
///   These are used to determine if a static-credentials profile is dangling.
/// - `keyring_has_secret_fn`: predicate that returns `true` when the DBFlux
///   keyring holds a secret for the given auth-profile ID. Used to distinguish
///   `keyring-only` from `file-gone` dangling profiles.
pub fn run_aws_config_reflect_migration(
    runtime: &StorageRuntime,
    all_config_names: &HashSet<String>,
    credentials_names: &HashSet<String>,
    keyring_has_secret_fn: impl Fn(&str) -> bool,
) -> Result<MigrationSummary, StorageError> {
    let meta_repo = runtime.app_meta();

    // Check and set the idempotency marker.
    if meta_repo.is_flag_set(MIGRATION_MARKER_KEY)? {
        info!("aws_config_reflect_migration: marker already set — skipping");
        return Ok(MigrationSummary::default());
    }

    let auth_repo = runtime.auth_profiles();
    let stored_rows = auth_repo.all()?;

    // Only consider rows for AWS provider IDs.
    let aws_rows: Vec<_> = stored_rows
        .into_iter()
        .filter(|row| AWS_PROVIDER_IDS.contains(&row.provider_id.as_str()))
        .collect();

    // Case-insensitive lookup from a section name to its canonical (file-cased)
    // form. Reflection derives the profile UUID from the file section name, so
    // rebinding to the canonical casing keeps connection bindings resolvable.
    let canonical_by_lower: HashMap<String, String> = all_config_names
        .iter()
        .chain(credentials_names.iter())
        .map(|n| (n.to_lowercase(), n.clone()))
        .collect();

    let mut summary = MigrationSummary::default();

    for row in aws_rows {
        // The real AWS section name lives in the `profile_name` field. The
        // display `name` may be decorated (the old import flow prefixed it with
        // "AWS "), so matching or deriving the UUID from it would mark every
        // imported profile dangling and leave its connections unbound.
        let stored_fields = auth_repo.get_fields(&row.id).unwrap_or_default();
        let aws_name = stored_fields
            .get("profile_name")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| row.name.trim().to_string());

        match canonical_by_lower.get(&aws_name.to_lowercase()) {
            Some(canonical_name) => {
                let new_provider_id = reflected_provider_id(&row.provider_id);
                let new_id = aws_profile_uuid(new_provider_id, canonical_name);
                let new_id_str = new_id.to_string();

                // Perform transactional rebind: update all connection rows that
                // reference the old UUID, then delete the stored auth-profile row.
                let connections_rebound = rebind_and_delete(runtime, &row.id, &new_id_str)?;

                // Log orphaned keyring entry notice (do NOT delete it).
                if keyring_has_secret_fn(&row.id) {
                    info!(
                        "aws_config_reflect_migration: profile '{}' (id={}) rebound to {}; \
                         a keyring secret for the old ID is now orphaned and can be cleaned up \
                         via a future consent-gated action",
                        aws_name, row.id, new_id_str
                    );
                }

                info!(
                    "aws_config_reflect_migration: rebound '{}' {} -> {} ({} connections)",
                    aws_name, row.id, new_id_str, connections_rebound
                );

                summary.rebound.push(ProfileMigrationOutcome::Rebound {
                    old_id: row.id,
                    new_id: new_id_str,
                    connections_rebound,
                });
            }
            None => {
                // Not found in config files — determine dangling origin.
                let is_static = row.provider_id == "aws-static-credentials";
                let has_secret = keyring_has_secret_fn(&row.id);

                let origin = if is_static && has_secret {
                    DanglingOrigin::KeyringOnly
                } else {
                    DanglingOrigin::FileGone
                };

                warn!(
                    "aws_config_reflect_migration: profile '{}' (id={}) not found in AWS config \
                     files — marked dangling (origin={})",
                    aws_name,
                    row.id,
                    origin.as_str()
                );

                auth_repo.set_dangling_origin(&row.id, origin.as_str())?;

                summary
                    .dangling
                    .push(ProfileMigrationOutcome::Dangling { id: row.id, origin });
            }
        }
    }

    // Set the idempotency marker.
    meta_repo.set_flag(MIGRATION_MARKER_KEY)?;

    info!(
        "aws_config_reflect_migration: done — {} rebound, {} dangling",
        summary.rebound.len(),
        summary.dangling.len()
    );

    Ok(summary)
}

/// Rebinds all `cfg_connection_profiles.auth_profile_id` rows that reference
/// `old_id` to `new_id`, then deletes the `cfg_auth_profiles` row for `old_id`.
///
/// All SQL operations are wrapped in a single transaction. If verification fails
/// (rebound count != referencing count before commit), the transaction is rolled
/// back and an error is returned.
///
/// Foreign key enforcement is temporarily disabled for this connection because
/// the new `auth_profile_id` values are deterministic UUIDs for virtual/reflected
/// profiles — they do not have rows in `cfg_auth_profiles`. FK enforcement is
/// restored after the commit (or rollback). This is safe because:
/// - The SQLite pragma applies only to this connection.
/// - The operation is atomic: either all rebinds + the delete commit, or nothing.
fn rebind_and_delete(
    runtime: &StorageRuntime,
    old_id: &str,
    new_id: &str,
) -> Result<usize, StorageError> {
    // Use a fresh raw connection for the transaction to avoid shared-connection
    // contention with repository-level connections.
    let conn = runtime.open_dbflux_db()?;

    // Disable FK enforcement before the transaction (SQLite requires this to be
    // done outside any active transaction).
    conn.pragma_update(None, "foreign_keys", "OFF")
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

    let tx = conn
        .unchecked_transaction()
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

    // Count connections referencing the old auth-profile ID.
    let referencing_count: usize = tx
        .query_row(
            "SELECT COUNT(*) FROM cfg_connection_profiles WHERE auth_profile_id = ?1",
            [old_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n as usize)
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

    // Rebind connection profiles.
    let rebound = tx
        .execute(
            "UPDATE cfg_connection_profiles SET auth_profile_id = ?2 WHERE auth_profile_id = ?1",
            [old_id, new_id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

    // Verify: rebound count must match referencing count before commit.
    if rebound != referencing_count {
        tx.rollback().ok();
        return Err(StorageError::Migration {
            kind: "aws_config_reflect_migration".to_string(),
            details: format!(
                "rebind count mismatch for old_id={}: expected {}, got {}",
                old_id, referencing_count, rebound
            ),
        });
    }

    // Also rebind access_params rows that reference the profile via the
    // `auth_profile_id` param (for Managed access kind). These are string
    // params in cfg_connection_profile_access_params; we update by value.
    tx.execute(
        "UPDATE cfg_connection_profile_access_params \
         SET param_value = ?2 \
         WHERE param_key = 'auth_profile_id' AND param_value = ?1",
        [old_id, new_id],
    )
    .map_err(|source| StorageError::Sqlite {
        path: "dbflux.db".into(),
        source,
    })?;

    // Delete the now-redundant stored auth-profile row.
    // Child fields in cfg_auth_profile_fields are deleted automatically via
    // the ON DELETE CASCADE foreign key defined in migration 001.
    tx.execute("DELETE FROM cfg_auth_profiles WHERE id = ?1", [old_id])
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

    tx.commit().map_err(|source| StorageError::Sqlite {
        path: "dbflux.db".into(),
        source,
    })?;

    // Re-enable FK enforcement after the transaction completes.
    // Failure here is non-fatal for the migration itself (the connection will
    // be dropped after this function returns), but log it for visibility.
    if let Err(e) = conn.pragma_update(None, "foreign_keys", "ON") {
        log::warn!(
            "aws_config_reflect_migration: failed to re-enable foreign_keys after rebind: {}",
            e
        );
    }

    Ok(rebound)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_storage::repositories::auth_profiles::AuthProfileDto;
    use dbflux_storage::repositories::connection_profiles::ConnectionProfileDto;
    use uuid::Uuid;

    fn open_runtime() -> StorageRuntime {
        StorageRuntime::in_memory().expect("in-memory runtime")
    }

    fn insert_auth_profile(runtime: &StorageRuntime, id: &str, name: &str, provider_id: &str) {
        let repo = runtime.auth_profiles();
        repo.insert(&AuthProfileDto {
            id: id.to_string(),
            name: name.to_string(),
            provider_id: provider_id.to_string(),
            enabled: true,
            created_at: String::new(),
            updated_at: String::new(),
            dangling_origin: None,
        })
        .expect("insert auth profile");
    }

    fn insert_connection(runtime: &StorageRuntime, conn_id: &str, auth_profile_id: &str) {
        let repo = runtime.connection_profiles();
        repo.insert(&ConnectionProfileDto {
            id: conn_id.to_string(),
            name: format!("connection-{}", conn_id),
            driver_id: Some("postgres".to_string()),
            description: None,
            favorite: false,
            color: None,
            icon: None,
            save_password: false,
            kind: Some("Postgres".to_string()),
            access_kind: Some("direct".to_string()),
            access_provider: None,
            auth_profile_id: Some(auth_profile_id.to_string()),
            proxy_profile_id: None,
            ssh_tunnel_profile_id: None,
            created_at: String::new(),
            updated_at: String::new(),
        })
        .expect("insert connection");
    }

    fn no_keyring(_id: &str) -> bool {
        false
    }

    fn has_keyring(target_id: &str) -> impl Fn(&str) -> bool + '_ {
        move |id: &str| id == target_id
    }

    fn names(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // T-4.1: marker absent → runs; marker present → no-op.
    #[test]
    fn marker_absent_migration_runs() {
        let runtime = open_runtime();
        insert_auth_profile(&runtime, "old-id-1", "dev", "aws-sso");

        let result =
            run_aws_config_reflect_migration(&runtime, &names(&["dev"]), &names(&[]), no_keyring)
                .expect("migration");

        // Should have rebound or processed the profile.
        assert_eq!(result.rebound.len() + result.dangling.len(), 1);

        // Marker must be set now.
        let meta = runtime.app_meta();
        assert!(
            meta.is_flag_set(MIGRATION_MARKER_KEY).expect("query"),
            "migration marker must be set after run"
        );
    }

    #[test]
    fn marker_present_migration_is_no_op() {
        let runtime = open_runtime();
        // Pre-set the marker.
        runtime
            .app_meta()
            .set_flag(MIGRATION_MARKER_KEY)
            .expect("set flag");

        insert_auth_profile(&runtime, "old-id-2", "prod", "aws-sso");

        let result =
            run_aws_config_reflect_migration(&runtime, &names(&["prod"]), &names(&[]), no_keyring)
                .expect("migration");

        // No-op: nothing touched.
        assert_eq!(result.rebound.len(), 0, "no rebind on second run");
        assert_eq!(result.dangling.len(), 0, "no dangling on second run");

        // The stored profile must still exist.
        let auth_repo = runtime.auth_profiles();
        let all = auth_repo.all().expect("list");
        assert_eq!(all.len(), 1, "stored row must be untouched");
    }

    // T-4.2: empty rows → no-op, marker set.
    #[test]
    fn empty_aws_rows_is_noop() {
        let runtime = open_runtime();

        let result =
            run_aws_config_reflect_migration(&runtime, &names(&[]), &names(&[]), no_keyring)
                .expect("migration");

        assert_eq!(result.rebound.len(), 0);
        assert_eq!(result.dangling.len(), 0);

        let meta = runtime.app_meta();
        assert!(meta.is_flag_set(MIGRATION_MARKER_KEY).expect("flag"));
    }

    // T-4.2: matched SSO row → rebind + delete stored row.
    #[test]
    fn matched_sso_row_is_rebound_and_deleted() {
        let runtime = open_runtime();
        let old_id = Uuid::new_v4().to_string();
        insert_auth_profile(&runtime, &old_id, "prod-sso", "aws-sso");

        // Two connections reference the old UUID.
        let conn1_id = Uuid::new_v4().to_string();
        let conn2_id = Uuid::new_v4().to_string();
        insert_connection(&runtime, &conn1_id, &old_id);
        insert_connection(&runtime, &conn2_id, &old_id);

        let expected_new_id = aws_profile_uuid("aws-sso", "prod-sso").to_string();

        let result = run_aws_config_reflect_migration(
            &runtime,
            &names(&["prod-sso"]),
            &names(&[]),
            no_keyring,
        )
        .expect("migration");

        assert_eq!(result.rebound.len(), 1, "one profile should be rebound");
        assert_eq!(result.dangling.len(), 0);

        let outcome = &result.rebound[0];
        match outcome {
            ProfileMigrationOutcome::Rebound {
                old_id: oid,
                new_id: nid,
                connections_rebound,
            } => {
                assert_eq!(oid, &old_id);
                assert_eq!(nid, &expected_new_id);
                assert_eq!(*connections_rebound, 2);
            }
            other => panic!("expected Rebound, got {:?}", other),
        }

        // Stored auth-profile row must be deleted.
        let auth_repo = runtime.auth_profiles();
        let all = auth_repo.all().expect("list");
        assert!(
            all.iter().all(|p| p.id != old_id),
            "old stored row must be deleted after rebind"
        );

        // Connections must now reference the new deterministic UUID.
        let conn_repo = runtime.connection_profiles();
        let conn1 = conn_repo.get(&conn1_id).expect("get").expect("exists");
        assert_eq!(
            conn1.auth_profile_id.as_deref(),
            Some(expected_new_id.as_str())
        );

        let conn2 = conn_repo.get(&conn2_id).expect("get").expect("exists");
        assert_eq!(
            conn2.auth_profile_id.as_deref(),
            Some(expected_new_id.as_str())
        );
    }

    // Regression: the display name is decorated (e.g. an "AWS " prefix from the
    // old import flow) while the real section name lives in the `profile_name`
    // field. Matching and UUID derivation must use the field, otherwise the
    // profile is wrongly marked dangling and its connections lose their binding.
    #[test]
    fn decorated_display_name_matches_via_profile_name_field() {
        let runtime = open_runtime();
        let old_id = Uuid::new_v4().to_string();
        insert_auth_profile(&runtime, &old_id, "AWS dev-sso", "aws-sso");

        let mut fields = HashMap::new();
        fields.insert("profile_name".to_string(), "dev-sso".to_string());
        runtime
            .auth_profiles()
            .set_fields(&old_id, &fields)
            .expect("set profile_name field");

        let conn_id = Uuid::new_v4().to_string();
        insert_connection(&runtime, &conn_id, &old_id);

        let expected_new_id = aws_profile_uuid("aws-sso", "dev-sso").to_string();

        let result = run_aws_config_reflect_migration(
            &runtime,
            &names(&["dev-sso"]),
            &names(&[]),
            no_keyring,
        )
        .expect("migration");

        assert_eq!(
            result.rebound.len(),
            1,
            "profile must rebind via its profile_name field, not the display name"
        );
        assert_eq!(result.dangling.len(), 0, "must not be marked dangling");

        let conn = runtime
            .connection_profiles()
            .get(&conn_id)
            .expect("get")
            .expect("exists");
        assert_eq!(
            conn.auth_profile_id.as_deref(),
            Some(expected_new_id.as_str()),
            "connection must rebind to the reflected deterministic UUID"
        );
    }

    // T-4.2: unmatched SSO row → dangling, connection UUID unchanged.
    #[test]
    fn unmatched_sso_row_marked_dangling() {
        let runtime = open_runtime();
        let old_id = Uuid::new_v4().to_string();
        insert_auth_profile(&runtime, &old_id, "old-env", "aws-sso");

        let conn_id = Uuid::new_v4().to_string();
        insert_connection(&runtime, &conn_id, &old_id);

        let result = run_aws_config_reflect_migration(
            &runtime,
            &names(&[]), // no matching section
            &names(&[]),
            no_keyring,
        )
        .expect("migration");

        assert_eq!(result.rebound.len(), 0);
        assert_eq!(result.dangling.len(), 1, "one profile should be dangling");

        // Stored row must still exist.
        let auth_repo = runtime.auth_profiles();
        let dto = auth_repo
            .get(&old_id)
            .expect("get")
            .expect("row must exist");
        assert_eq!(dto.dangling_origin.as_deref(), Some("file-gone"));

        // Connection must still reference the old UUID.
        let conn_repo = runtime.connection_profiles();
        let conn = conn_repo.get(&conn_id).expect("get").expect("exists");
        assert_eq!(conn.auth_profile_id.as_deref(), Some(old_id.as_str()));
    }

    // T-4.2: idempotency — second run is a no-op.
    #[test]
    fn second_run_is_noop() {
        let runtime = open_runtime();
        insert_auth_profile(&runtime, &Uuid::new_v4().to_string(), "dev", "aws-sso");

        run_aws_config_reflect_migration(&runtime, &names(&["dev"]), &names(&[]), no_keyring)
            .expect("first run");

        // After the first run the stored row is deleted and the marker is set.
        // A second run should be a no-op.
        let result2 =
            run_aws_config_reflect_migration(&runtime, &names(&["dev"]), &names(&[]), no_keyring)
                .expect("second run");

        assert_eq!(result2.rebound.len(), 0, "second run must not rebind");
        assert_eq!(
            result2.dangling.len(),
            0,
            "second run must not mark dangling"
        );
    }

    // T-4.3: aws-static-credentials row → rebind to aws-shared-credentials UUID.
    #[test]
    fn static_credentials_row_rebinds_to_shared_credentials_uuid() {
        let runtime = open_runtime();
        let old_id = Uuid::new_v4().to_string();
        insert_auth_profile(&runtime, &old_id, "deploy", "aws-static-credentials");

        let conn_id = Uuid::new_v4().to_string();
        insert_connection(&runtime, &conn_id, &old_id);

        // The credentials file has a matching entry.
        let expected_new_id = aws_profile_uuid("aws-shared-credentials", "deploy").to_string();

        let result = run_aws_config_reflect_migration(
            &runtime,
            &names(&[]),
            &names(&["deploy"]), // found in credentials file
            no_keyring,
        )
        .expect("migration");

        assert_eq!(result.rebound.len(), 1);

        let outcome = &result.rebound[0];
        match outcome {
            ProfileMigrationOutcome::Rebound { new_id, .. } => {
                assert_eq!(
                    new_id, &expected_new_id,
                    "static-credentials must rebind to aws-shared-credentials UUID"
                );
            }
            other => panic!("expected Rebound, got {:?}", other),
        }

        // Connection must reference the new UUID.
        let conn_repo = runtime.connection_profiles();
        let conn = conn_repo.get(&conn_id).expect("get").expect("exists");
        assert_eq!(
            conn.auth_profile_id.as_deref(),
            Some(expected_new_id.as_str())
        );
    }

    // T-4.4: matched static profile → keyring preserved (not deleted), orphan logged.
    // We verify the test keyring predicate is called and the profile IS rebound.
    #[test]
    fn matched_static_profile_keyring_preserved() {
        let runtime = open_runtime();
        let old_id = Uuid::new_v4().to_string();
        insert_auth_profile(&runtime, &old_id, "ci-user", "aws-static-credentials");

        let old_id_clone = old_id.clone();
        // The Fn closure can't capture-and-mutate a local, so track the probe via Arc<Mutex>.
        let checked = Arc::new(std::sync::Mutex::new(false));
        let checked_clone = checked.clone();

        let result = run_aws_config_reflect_migration(
            &runtime,
            &names(&[]),
            &names(&["ci-user"]), // matched in credentials file
            move |id: &str| {
                if id == old_id_clone {
                    *checked_clone.lock().unwrap() = true;
                    true // has keyring secret
                } else {
                    false
                }
            },
        )
        .expect("migration");

        // Profile was matched and rebound.
        assert_eq!(result.rebound.len(), 1, "profile must be rebound");
        // The keyring predicate was called for the profile's old ID.
        assert!(
            *checked.lock().unwrap(),
            "keyring predicate must be called for matched profile"
        );

        // Stored row deleted after rebind.
        let auth_repo = runtime.auth_profiles();
        assert!(
            auth_repo.get(&old_id).expect("get").is_none(),
            "matched profile's stored row must be deleted"
        );
        // (Actual keyring deletion is NOT performed — no assertion for deletion
        //  needed since we never delete. If the test keyring has the entry, it
        //  must still have it — but we are not simulating a real keyring here.)
    }

    // T-4.5: dangling keyring-only static profile → secret NOT deleted, row kept.
    #[test]
    fn dangling_keyring_only_static_profile_preserved() {
        let runtime = open_runtime();
        let old_id = Uuid::new_v4().to_string();
        insert_auth_profile(&runtime, &old_id, "legacy-key", "aws-static-credentials");

        let conn_id = Uuid::new_v4().to_string();
        insert_connection(&runtime, &conn_id, &old_id);

        let result = run_aws_config_reflect_migration(
            &runtime,
            &names(&[]),          // not in config
            &names(&[]),          // not in credentials
            has_keyring(&old_id), // has keyring secret
        )
        .expect("migration");

        assert_eq!(result.dangling.len(), 1);
        match &result.dangling[0] {
            ProfileMigrationOutcome::Dangling { id, origin } => {
                assert_eq!(id, &old_id);
                assert_eq!(*origin, DanglingOrigin::KeyringOnly);
            }
            other => panic!("expected Dangling, got {:?}", other),
        }

        // Row must be kept.
        let auth_repo = runtime.auth_profiles();
        let dto = auth_repo.get(&old_id).expect("get").expect("must exist");
        assert_eq!(
            dto.dangling_origin.as_deref(),
            Some("keyring-only"),
            "dangling_origin must be 'keyring-only'"
        );

        // Connection must retain old UUID.
        let conn_repo = runtime.connection_profiles();
        let conn = conn_repo.get(&conn_id).expect("get").expect("exists");
        assert_eq!(
            conn.auth_profile_id.as_deref(),
            Some(old_id.as_str()),
            "connection must keep old UUID for dangling profile"
        );
    }

    // T-4.6: dangling non-static profile (SSO name no longer in file).
    #[test]
    fn dangling_non_static_sso_profile_preserved() {
        let runtime = open_runtime();
        let old_id = Uuid::new_v4().to_string();
        insert_auth_profile(&runtime, &old_id, "old-env", "aws-sso-session");

        let conn_id = Uuid::new_v4().to_string();
        insert_connection(&runtime, &conn_id, &old_id);

        let result = run_aws_config_reflect_migration(
            &runtime,
            &names(&[]), // not in config (deleted from ~/.aws/config)
            &names(&[]),
            no_keyring,
        )
        .expect("migration");

        assert_eq!(result.dangling.len(), 1);
        match &result.dangling[0] {
            ProfileMigrationOutcome::Dangling { id, origin } => {
                assert_eq!(id, &old_id);
                assert_eq!(*origin, DanglingOrigin::FileGone);
            }
            other => panic!("expected Dangling, got {:?}", other),
        }

        // Row kept, marked file-gone.
        let auth_repo = runtime.auth_profiles();
        let dto = auth_repo.get(&old_id).expect("get").expect("must exist");
        assert_eq!(dto.dangling_origin.as_deref(), Some("file-gone"));

        // Connection UUID unchanged.
        let conn_repo = runtime.connection_profiles();
        let conn = conn_repo.get(&conn_id).expect("get").expect("exists");
        assert_eq!(conn.auth_profile_id.as_deref(), Some(old_id.as_str()));
    }

    // Integration test: all four outcome types in one run.
    #[test]
    fn integration_all_four_outcome_types() {
        let runtime = open_runtime();

        // 1. Matched SSO profile → will be rebound.
        let matched_sso_id = Uuid::new_v4().to_string();
        insert_auth_profile(&runtime, &matched_sso_id, "dev", "aws-sso");
        let conn_sso_id = Uuid::new_v4().to_string();
        insert_connection(&runtime, &conn_sso_id, &matched_sso_id);

        // 2. Matched credentials-file profile → rebound (aws-static → aws-shared).
        let matched_cred_id = Uuid::new_v4().to_string();
        insert_auth_profile(
            &runtime,
            &matched_cred_id,
            "ci-deploy",
            "aws-static-credentials",
        );

        // 3. Dangling keyring-only static profile.
        let dangling_keyring_id = Uuid::new_v4().to_string();
        insert_auth_profile(
            &runtime,
            &dangling_keyring_id,
            "legacy-secret",
            "aws-static-credentials",
        );

        // 4. Dangling non-static SSO-session profile.
        let dangling_sso_session_id = Uuid::new_v4().to_string();
        insert_auth_profile(
            &runtime,
            &dangling_sso_session_id,
            "gone-session",
            "aws-sso-session",
        );

        let dangling_keyring_clone = dangling_keyring_id.clone();

        let result = run_aws_config_reflect_migration(
            &runtime,
            &names(&["dev"]),       // only "dev" found in config
            &names(&["ci-deploy"]), // "ci-deploy" found in credentials
            move |id: &str| id == dangling_keyring_clone, // only "legacy-secret" has a keyring secret
        )
        .expect("migration");

        // Two profiles should be rebound.
        assert_eq!(result.rebound.len(), 2, "two profiles must be rebound");

        // Two profiles should be dangling.
        assert_eq!(result.dangling.len(), 2, "two profiles must be dangling");

        // Verify dangling origins.
        let keyring_only = result.dangling.iter().find(|d| match d {
            ProfileMigrationOutcome::Dangling { id, .. } => id == &dangling_keyring_id,
            _ => false,
        });
        assert!(
            keyring_only.is_some(),
            "dangling_keyring_id must appear in dangling list"
        );
        match keyring_only.unwrap() {
            ProfileMigrationOutcome::Dangling { origin, .. } => {
                assert_eq!(*origin, DanglingOrigin::KeyringOnly);
            }
            _ => unreachable!(),
        }

        let file_gone = result.dangling.iter().find(|d| match d {
            ProfileMigrationOutcome::Dangling { id, .. } => id == &dangling_sso_session_id,
            _ => false,
        });
        assert!(
            file_gone.is_some(),
            "dangling_sso_session_id must appear in dangling list"
        );
        match file_gone.unwrap() {
            ProfileMigrationOutcome::Dangling { origin, .. } => {
                assert_eq!(*origin, DanglingOrigin::FileGone);
            }
            _ => unreachable!(),
        }

        // Marker must be set.
        assert!(
            runtime
                .app_meta()
                .is_flag_set(MIGRATION_MARKER_KEY)
                .expect("flag"),
            "migration marker must be set after run"
        );

        // SSO connection must be rebound to deterministic UUID.
        let expected_sso_id = aws_profile_uuid("aws-sso", "dev").to_string();
        let conn_repo = runtime.connection_profiles();
        let sso_conn = conn_repo.get(&conn_sso_id).expect("get").expect("exists");
        assert_eq!(
            sso_conn.auth_profile_id.as_deref(),
            Some(expected_sso_id.as_str()),
            "SSO connection must reference deterministic UUID"
        );
    }
}
