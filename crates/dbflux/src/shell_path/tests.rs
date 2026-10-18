use super::*;
use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

/// Writes an executable stand-in for a login shell. It receives the same
/// `-l -i -c <command>` arguments as a real shell and runs `body` with `sh`.
fn fake_shell(directory: &Path, body: &str) -> PathBuf {
    let path = directory.join("fake-shell");
    fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("write fake shell");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod fake shell");
    path
}

#[test]
fn extract_marked_path_ignores_output_around_the_markers() {
    let output = format!(
        "Last login: Sun Oct 18\nwelcome banner\n{PATH_START_MARKER}/opt/homebrew/bin:/usr/bin{PATH_END_MARKER}\nprompt junk"
    );

    let path = extract_marked_path(output.as_bytes());

    assert_eq!(
        path.as_deref(),
        Some(OsStr::new("/opt/homebrew/bin:/usr/bin"))
    );
}

#[test]
fn extract_marked_path_rejects_missing_or_empty_values() {
    assert_eq!(extract_marked_path(b"/usr/bin:/bin"), None);
    assert_eq!(
        extract_marked_path(format!("{PATH_START_MARKER}/usr/bin").as_bytes()),
        None
    );
    assert_eq!(
        extract_marked_path(format!("{PATH_START_MARKER}{PATH_END_MARKER}").as_bytes()),
        None
    );
}

#[test]
fn merge_paths_puts_login_entries_first_and_keeps_missing_current_entries() {
    let merged = merge_paths(
        OsStr::new("/opt/homebrew/bin:/usr/bin:/bin"),
        Some(OsStr::new("/usr/bin:/bin:/usr/sbin:/sbin")),
    );

    assert_eq!(
        merged.as_deref(),
        Some(OsStr::new(
            "/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"
        ))
    );
}

#[test]
fn merge_paths_drops_duplicate_and_empty_entries() {
    let merged = merge_paths(
        OsStr::new("/opt/homebrew/bin::/opt/homebrew/bin:/usr/bin"),
        None,
    );

    assert_eq!(
        merged.as_deref(),
        Some(OsStr::new("/opt/homebrew/bin:/usr/bin"))
    );
}

#[test]
fn capture_reads_path_from_the_shell_command_despite_startup_noise() {
    let directory = tempfile::tempdir().expect("temp dir");
    let shell = fake_shell(
        directory.path(),
        "echo 'rc file noise'\nPATH=\"/from/login/shell:$PATH\" exec /bin/sh -c \"$4\"",
    );

    let path = capture_login_shell_path(&shell, Duration::from_secs(5)).expect("captured path");

    let entries: Vec<PathBuf> = std::env::split_paths(&path).collect();
    assert_eq!(
        entries.first().map(PathBuf::as_path),
        Some(Path::new("/from/login/shell"))
    );
}

#[test]
fn capture_gives_up_when_the_shell_does_not_finish_in_time() {
    let directory = tempfile::tempdir().expect("temp dir");
    let shell = fake_shell(directory.path(), "exec sleep 30");

    let started = Instant::now();
    let result = capture_login_shell_path(&shell, Duration::from_millis(300));

    assert!(result.is_err(), "expected a timeout, got {result:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "timeout was not enforced: {:?}",
        started.elapsed()
    );
}

#[test]
fn capture_fails_when_the_shell_does_not_print_the_markers() {
    let directory = tempfile::tempdir().expect("temp dir");
    let shell = fake_shell(directory.path(), "echo 'no markers here'");

    let result = capture_login_shell_path(&shell, Duration::from_secs(5));

    assert!(result.is_err(), "expected a failure, got {result:?}");
}

#[test]
fn capture_fails_when_the_shell_cannot_be_started() {
    let result = capture_login_shell_path(
        Path::new("/nonexistent/dbflux-test-shell"),
        Duration::from_secs(5),
    );

    assert!(result.is_err(), "expected a spawn failure, got {result:?}");
}
