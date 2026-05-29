//! File-dialog availability probe and export fallback path.
//!
//! `rfd::AsyncFileDialog::save_file()` returns `Option<FileHandle>` and cannot
//! distinguish "user cancelled" from "backend failed" (no portal, no zenity/
//! kdialog). To avoid silent failures on Linux when the host has neither a
//! working XDG desktop portal nor a zenity/kdialog fallback installed, callers
//! probe `is_native_file_dialog_available()` before invoking rfd and route to
//! `fallback_export_dir()` when the probe fails.

use std::path::PathBuf;

/// Returns `true` when a native file picker is expected to work on this host.
///
/// On Windows and macOS this is unconditionally `true` — the native pickers
/// are part of the OS.
///
/// On Linux this is a heuristic that succeeds when at least one of the
/// following binaries is on `PATH`:
/// - `xdg-desktop-portal` (the XDG portal backend rfd prefers)
/// - `zenity` (rfd's fallback when the portal call fails)
/// - `kdialog` (KDE's equivalent fallback)
///
/// The heuristic is intentionally lenient: rfd may still fail at runtime when
/// the portal binary exists but the FileChooser interface is unimplemented by
/// the active desktop. The Linux user pain we're solving — a missing portal AND
/// missing zenity, which makes rfd return `None` silently — is reliably caught
/// by this probe.
pub fn is_native_file_dialog_available() -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        true
    }

    #[cfg(target_os = "linux")]
    {
        binary_on_path("xdg-desktop-portal")
            || binary_on_path("zenity")
            || binary_on_path("kdialog")
    }
}

#[cfg(target_os = "linux")]
fn binary_on_path(bin: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };

    std::env::split_paths(&paths).any(|dir| {
        let candidate = dir.join(bin);
        candidate.is_file()
    })
}

/// Returns the fallback directory for exports written when no native file
/// picker is available, creating it if missing.
///
/// Resolves to `~/.local/share/dbflux/exports/` via
/// `dbflux_storage::paths::data_dir()`.
pub fn fallback_export_dir() -> Result<PathBuf, String> {
    let data_dir = dbflux_storage::paths::data_dir()
        .map_err(|e| format!("Failed to resolve data directory: {}", e))?;

    let exports = data_dir.join("exports");

    std::fs::create_dir_all(&exports)
        .map_err(|e| format!("Failed to create exports directory {}: {}", exports.display(), e))?;

    Ok(exports)
}

/// Builds a non-clobbering path inside `dir` for `filename`. If the file
/// already exists, suffixes `-2`, `-3`, ... before the extension until a free
/// slot is found.
pub fn unique_path_in(dir: &std::path::Path, filename: &str) -> PathBuf {
    let initial = dir.join(filename);
    if !initial.exists() {
        return initial;
    }

    let (stem, ext) = match filename.rfind('.') {
        Some(i) if i > 0 => (&filename[..i], Some(&filename[i + 1..])),
        _ => (filename, None),
    };

    for n in 2..u32::MAX {
        let candidate_name = match ext {
            Some(ext) => format!("{}-{}.{}", stem, n, ext),
            None => format!("{}-{}", stem, n),
        };
        let candidate = dir.join(candidate_name);
        if !candidate.exists() {
            return candidate;
        }
    }

    initial
}
