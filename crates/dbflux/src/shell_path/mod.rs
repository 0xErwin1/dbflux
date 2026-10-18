//! Imports the login shell's `PATH` into a desktop launch.
//!
//! An app started from Finder, the Dock or a `.desktop` entry inherits the
//! session manager's minimal `PATH` rather than the one the user's shell
//! configuration builds. Child processes such as `aws` (and the
//! `session-manager-plugin` it looks up in turn) are then not found, so the
//! whole process `PATH` is corrected instead of resolving single binaries.

use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::IsTerminal;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PATH_START_MARKER: &str = "_DBFLUX_PATH_START_";
const PATH_END_MARKER: &str = "_DBFLUX_PATH_END_";

/// Bounds how long a slow or prompting shell configuration can delay startup.
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(5);
const CAPTURE_POLL_INTERVAL: Duration = Duration::from_millis(20);

static CAPTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Result of the startup import, reported once logging is available.
pub(crate) enum ShellPathImport {
    LaunchedFromTerminal,
    NoLoginShell,
    Imported { shell: PathBuf },
    Failed { shell: PathBuf, reason: String },
}

impl ShellPathImport {
    pub(crate) fn log(&self) {
        match self {
            Self::LaunchedFromTerminal => {
                log::debug!("Launched from a terminal, keeping the inherited PATH");
            }
            Self::NoLoginShell => {
                log::warn!("No login shell found, keeping the inherited PATH");
            }
            Self::Imported { shell } => {
                log::info!("Imported PATH from login shell {}", shell.display());
            }
            Self::Failed { shell, reason } => {
                log::warn!(
                    "Could not import PATH from login shell {}: {reason}. Keeping the inherited PATH",
                    shell.display()
                );
            }
        }
    }
}

/// Replaces the process `PATH` with the login shell's, keeping inherited
/// entries the shell does not list.
///
/// Must run before the process starts any other thread: it calls
/// `std::env::set_var`, which is unsound while another thread may read the
/// environment.
#[expect(
    unsafe_code,
    reason = "`std::env::set_var` is unsafe in edition 2024; the caller runs \
              this before any other thread exists, so nothing reads the \
              environment concurrently"
)]
pub(crate) fn import_login_shell_path() -> ShellPathImport {
    if std::io::stdin().is_terminal() {
        return ShellPathImport::LaunchedFromTerminal;
    }

    let Some(shell) = login_shell() else {
        return ShellPathImport::NoLoginShell;
    };

    let login_path = match capture_login_shell_path(&shell, CAPTURE_TIMEOUT) {
        Ok(path) => path,
        Err(reason) => return ShellPathImport::Failed { shell, reason },
    };

    let current_path = std::env::var_os("PATH");
    let Some(merged) = merge_paths(&login_path, current_path.as_deref()) else {
        return ShellPathImport::Failed {
            shell,
            reason: "the merged PATH could not be joined".to_string(),
        };
    };

    unsafe { std::env::set_var("PATH", merged) };

    ShellPathImport::Imported { shell }
}

fn login_shell() -> Option<PathBuf> {
    std::env::var_os("SHELL")
        .filter(|shell| !shell.is_empty())
        .map(PathBuf::from)
        .or_else(shell_from_passwd)
}

/// Reads the user's shell from the password database, for launches that do
/// not set `SHELL`.
#[expect(
    unsafe_code,
    reason = "libc `getpwuid_r` FFI writes into a caller-owned `passwd` and \
              buffer that outlive the call; `pw_shell` is only read when the \
              call succeeded and returned a non-null entry, and it points into \
              that buffer"
)]
fn shell_from_passwd() -> Option<PathBuf> {
    let mut entry: libc::passwd = unsafe { std::mem::zeroed() };
    let mut buffer = vec![0 as libc::c_char; 4096];
    let mut result: *mut libc::passwd = std::ptr::null_mut();

    let status = unsafe {
        libc::getpwuid_r(
            libc::getuid(),
            &mut entry,
            buffer.as_mut_ptr(),
            buffer.len(),
            &mut result,
        )
    };

    if status != 0 || result.is_null() || entry.pw_shell.is_null() {
        return None;
    }

    let shell = unsafe { std::ffi::CStr::from_ptr(entry.pw_shell) };
    let shell = OsStr::from_bytes(shell.to_bytes());

    (!shell.is_empty()).then(|| PathBuf::from(shell))
}

/// Runs `shell` as an interactive login shell and returns the `PATH` it
/// builds.
///
/// Output goes to a temporary file rather than a pipe: a process the shell
/// configuration leaves running in the background can keep a pipe open, and
/// reading it would then never reach end of file.
fn capture_login_shell_path(shell: &Path, timeout: Duration) -> Result<OsString, String> {
    // Unique per call so concurrent captures in one process (tests) and a file
    // left behind by an earlier run never collide with `create_new`.
    let sequence = CAPTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    let output_path = std::env::temp_dir().join(format!(
        "dbflux-login-shell-path-{}-{sequence}-{nanos}",
        std::process::id()
    ));
    let output_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&output_path)
        .map_err(|error| format!("could not create {}: {error}", output_path.display()))?;

    let result = run_shell_into(shell, timeout, &output_file).and_then(|()| {
        let output = fs::read(&output_path)
            .map_err(|error| format!("could not read the shell output: {error}"))?;

        extract_marked_path(&output).ok_or_else(|| "the shell did not print a PATH".to_string())
    });

    if let Err(error) = fs::remove_file(&output_path) {
        log::debug!("Could not remove {}: {error}", output_path.display());
    }

    result
}

fn run_shell_into(shell: &Path, timeout: Duration, output_file: &File) -> Result<(), String> {
    let stdout = output_file
        .try_clone()
        .map_err(|error| format!("could not share the output file: {error}"))?;

    // The login shell hands off to `/bin/sh` so the expansion is the same
    // under zsh, bash and fish, whose `$PATH` is a list rather than a string.
    let command = format!(
        "exec /bin/sh -c 'printf \"%s\" \"{PATH_START_MARKER}${{PATH}}{PATH_END_MARKER}\"'"
    );

    let mut child = Command::new(shell)
        .args(["-l", "-i", "-c", &command])
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("could not start it: {error}"))?;

    let deadline = Instant::now() + timeout;

    loop {
        match child.try_wait() {
            Ok(Some(_status)) => return Ok(()),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(CAPTURE_POLL_INTERVAL),
            Ok(None) => {
                stop_child(&mut child);
                return Err(format!("it did not finish within {timeout:?}"));
            }
            Err(error) => {
                stop_child(&mut child);
                return Err(format!("could not wait for it: {error}"));
            }
        }
    }
}

fn stop_child(child: &mut std::process::Child) {
    if let Err(error) = child.kill() {
        log::debug!("Could not kill the login shell: {error}");
    }

    if let Err(error) = child.wait() {
        log::debug!("Could not reap the login shell: {error}");
    }
}

fn extract_marked_path(output: &[u8]) -> Option<OsString> {
    let start = find_bytes(output, PATH_START_MARKER.as_bytes())? + PATH_START_MARKER.len();
    let rest = output.get(start..)?;
    let end = find_bytes(rest, PATH_END_MARKER.as_bytes())?;
    let value = rest.get(..end)?;

    (!value.is_empty()).then(|| OsStr::from_bytes(value).to_os_string())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Joins the login shell's entries, in order, with the inherited entries it
/// does not list, dropping empty and repeated entries.
fn merge_paths(login: &OsStr, current: Option<&OsStr>) -> Option<OsString> {
    let mut entries: Vec<PathBuf> = Vec::new();
    let candidates =
        std::env::split_paths(login).chain(current.into_iter().flat_map(std::env::split_paths));

    for entry in candidates {
        if !entry.as_os_str().is_empty() && !entries.contains(&entry) {
            entries.push(entry);
        }
    }

    std::env::join_paths(entries).ok()
}

#[cfg(test)]
mod tests;
