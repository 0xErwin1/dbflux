use std::path::{Path, PathBuf};

use crate::error::StorageError;

/// Creates `path` (and parents) and, on Unix, restricts it to owner-only (`0o700`).
///
/// On non-Unix platforms the chmod is skipped; directory ACLs are managed by the OS.
/// Mirrors the owner-private creation pattern in `dbflux_ipc::auth`.
pub(crate) fn ensure_private_dir(path: &Path) -> Result<(), StorageError> {
    std::fs::create_dir_all(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    secure_dir_permissions(path)
}

/// On Unix, restricts an existing directory to `0o700`. No-op elsewhere.
#[cfg(unix)]
pub(crate) fn secure_dir_permissions(path: &Path) -> Result<(), StorageError> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path)
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(path, permissions).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(not(unix))]
pub(crate) fn secure_dir_permissions(_path: &Path) -> Result<(), StorageError> {
    Ok(())
}

/// On Unix, restricts an existing file to owner read/write only (`0o600`). No-op elsewhere.
///
/// Idempotent and safe to call right after the file is created/opened. There is a
/// brief open-then-chmod window (TOCTOU) where the file exists at the process umask
/// before narrowing; this is an accepted tradeoff, identical to `dbflux_ipc::auth`,
/// because rusqlite/`File::create` do not expose pre-creation mode.
pub fn secure_file_permissions(path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(path)
            .map_err(|source| StorageError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions).map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Secures the WAL and SHM sidecar files that SQLite creates lazily alongside a database.
///
/// SQLite creates `<db>-wal`, `<db>-shm`, and (in rollback-journal mode) `<db>-journal`
/// on the first write, after the database file itself has already been opened and
/// chmod'd. This means those sidecars appear at the process umask rather than at the
/// desired owner-only mode. This helper is called after the first write has occurred
/// (e.g. after migrations run) to narrow each sidecar that actually exists to `0o600`.
///
/// Missing sidecar files are not an error — when WAL mode is not active or the
/// database has never been written, the sidecar simply does not exist yet.
///
/// On non-Unix platforms this is a no-op.
pub fn secure_db_sidecars(db_path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        let base = db_path.as_os_str();
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar_os = base.to_owned();
            sidecar_os.push(suffix);
            let sidecar = Path::new(&sidecar_os);
            if sidecar.exists() {
                secure_file_permissions(sidecar)?;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = db_path;
    Ok(())
}

/// Returns `~/.local/share/dbflux/`, creating it if necessary with owner-only permissions.
///
/// This directory is used for:
/// - The unified `dbflux.db` database
/// - Session artifacts and scratch files
pub fn data_dir() -> Result<PathBuf, StorageError> {
    let base = dirs::data_dir().ok_or(StorageError::DataDirUnavailable)?;
    let dir = base.join("dbflux");
    ensure_private_dir(&dir)?;
    Ok(dir)
}

/// Marker file inside the data directory that opts a nightly build into the
/// stable database. Its presence is the whole signal — the file is empty.
///
/// This setting cannot live in the database itself: it decides *which* database
/// to open, so it must be readable before any database is opened.
fn shared_db_marker_path() -> Result<PathBuf, StorageError> {
    Ok(data_dir()?.join("use-stable-db"))
}

/// Whether the running nightly build is configured to share the stable
/// database instead of its own `dbflux-nightly.db`.
///
/// Always `false` outside nightly. Best-effort: a probing error is treated as
/// "not shared" so a filesystem hiccup never silently redirects writes onto the
/// stable database.
pub fn nightly_shares_stable_db() -> bool {
    if dbflux_core::ReleaseChannel::current() != dbflux_core::ReleaseChannel::Nightly {
        return false;
    }

    shared_db_marker_path()
        .map(|path| path.exists())
        .unwrap_or(false)
}

/// Opts the nightly build into (`enabled`) or out of (`!enabled`) the stable
/// database. The change takes effect on the next launch, since the database is
/// opened once at startup.
pub fn set_nightly_shares_stable_db(enabled: bool) -> Result<(), StorageError> {
    let path = shared_db_marker_path()?;

    if enabled {
        std::fs::write(&path, b"").map_err(|source| StorageError::Io {
            path: path.clone(),
            source,
        })?;
        secure_file_permissions(&path)?;
    } else if path.exists() {
        std::fs::remove_file(&path).map_err(|source| StorageError::Io {
            path: path.clone(),
            source,
        })?;
    }

    Ok(())
}

/// Returns the path for the unified database inside the data directory.
///
/// The file name is channel-specific: nightly builds use `dbflux-nightly.db`
/// so a pre-release migration cannot corrupt the stable `dbflux.db` of a user
/// who runs both channels side by side. A nightly build may opt into the stable
/// database via [`set_nightly_shares_stable_db`].
pub fn dbflux_db_path() -> Result<PathBuf, StorageError> {
    let file_name = if nightly_shares_stable_db() {
        "dbflux.db"
    } else {
        dbflux_core::ReleaseChannel::current().db_file_name()
    };
    Ok(data_dir()?.join(file_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn unique_tmp_path(prefix: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "dbflux_paths_test_{}_{}_{}",
            std::process::id(),
            n,
            prefix
        ))
    }

    #[cfg(unix)]
    #[test]
    fn secure_db_sidecars_sets_0o600_on_existing_sidecars() {
        use std::os::unix::fs::PermissionsExt;

        let db = unique_tmp_path("sidecars.db");
        let wal = {
            let mut p = db.as_os_str().to_owned();
            p.push("-wal");
            PathBuf::from(p)
        };
        let shm = {
            let mut p = db.as_os_str().to_owned();
            p.push("-shm");
            PathBuf::from(p)
        };

        // Create dummy sidecars at the process umask (typically 0o644).
        std::fs::write(&wal, b"").expect("write wal");
        std::fs::write(&shm, b"").expect("write shm");

        secure_db_sidecars(&db).expect("secure_db_sidecars should succeed");

        for (label, path) in [("wal", &wal), ("shm", &shm)] {
            let mode = std::fs::metadata(path)
                .expect("metadata readable")
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o777,
                0o600,
                "{} sidecar should be 0o600, got {:o}",
                label,
                mode & 0o777
            );
        }

        // Missing -journal must be silently tolerated (no error).
        let _ = std::fs::remove_file(&wal);
        let _ = std::fs::remove_file(&shm);
    }

    #[cfg(unix)]
    #[test]
    fn secure_db_sidecars_tolerates_missing_sidecars() {
        let db = unique_tmp_path("no_sidecars.db");
        // Nothing created — all sidecars absent.
        let result = secure_db_sidecars(&db);
        assert!(result.is_ok(), "missing sidecars should not be an error");
    }

    #[cfg(unix)]
    #[test]
    fn ensure_private_dir_sets_0o700() {
        use std::os::unix::fs::PermissionsExt;

        let path = unique_tmp_path("dir");
        let _ = std::fs::remove_dir_all(&path);

        ensure_private_dir(&path).expect("ensure_private_dir should succeed");

        let mode = std::fs::metadata(&path)
            .expect("metadata readable")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o700,
            "directory should be 0o700, got {:o}",
            mode & 0o777
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    #[cfg(unix)]
    #[test]
    fn secure_file_permissions_sets_0o600() {
        use std::os::unix::fs::PermissionsExt;

        let path = unique_tmp_path("file");
        std::fs::write(&path, b"test").expect("write temp file");

        secure_file_permissions(&path).expect("secure_file_permissions should succeed");

        let mode = std::fs::metadata(&path)
            .expect("metadata readable")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "file should be 0o600, got {:o}",
            mode & 0o777
        );

        let _ = std::fs::remove_file(&path);
    }
}
