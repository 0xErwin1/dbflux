//! Handing a local file to the operating system's default application.
//!
//! Used by surfaces that cannot render a file themselves (PDFs, archives, and
//! any other binary payload) and instead download it and let the desktop open
//! it. The spawn is deliberately fire-and-forget: the handler process outlives
//! the call and DBFlux never waits on it.

use dbflux_core::LogErr;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Directory holding files downloaded only so they can be opened by an
/// external application. Scoped under the OS temp dir so the platform's own
/// cleanup eventually reclaims it.
pub fn external_open_dir() -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join("dbflux-open");

    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create {}: {}", dir.display(), e))?;

    Ok(dir)
}

/// Opens `path` with the system's default handler for its type.
///
/// Returns `false` when the handler could not be spawned at all; a handler
/// that starts and then fails on its own is outside what the OS reports back
/// here, so a `true` return only means "the launch was accepted".
pub fn open_external(path: &Path) -> bool {
    let mut command = handler_command(path);

    command
        .spawn()
        .log_err_with("failed to open file with the system handler")
        .is_some()
}

#[cfg(target_os = "linux")]
fn handler_command(path: &Path) -> Command {
    let mut command = Command::new("xdg-open");
    command.arg(path);
    command
}

#[cfg(target_os = "macos")]
fn handler_command(path: &Path) -> Command {
    let mut command = Command::new("open");
    command.arg(path);
    command
}

#[cfg(target_os = "windows")]
fn handler_command(path: &Path) -> Command {
    // `start` is a cmd builtin, not an executable; its first quoted argument is
    // the window title, so an empty one must precede the path.
    let mut command = Command::new("cmd");
    command.arg("/C").arg("start").arg("").arg(path);
    command
}

#[cfg(test)]
mod tests {
    use super::external_open_dir;

    /// The scratch directory is created on demand so callers can write into it
    /// straight away.
    #[test]
    fn external_open_dir_is_created_on_demand() {
        let dir = external_open_dir().expect("scratch directory");

        assert!(dir.is_dir());
        assert!(dir.ends_with("dbflux-open"));
    }
}
