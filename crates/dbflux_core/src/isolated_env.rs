//! Test-only helper that runs individual tests in a hermetic child process.
//!
//! The parent re-executes the current test binary with a cleared environment,
//! forwarding only OS-essential variables plus explicit fixture variables.
//! The child recognizes itself by an exact test-name marker set by the parent
//! process (an identical caller-exported marker would also select child mode)
//! and executes its original body without mutating anything beyond its own
//! process.

use std::ffi::OsStr;
use std::process::Command;

const MARKER_VAR: &str = "DBFLUX_ISOLATED_ENV_FIXTURE";

/// OS variables required to locate and run executables on each platform.
const OS_ESSENTIAL_VARS: &[&str] = &["PATH", "SystemRoot", "SystemDrive", "TEMP", "TMP", "TMPDIR"];

/// Runs `test_name` in an isolated child process with a clean environment.
///
/// Returns `false` when the caller is the isolated child (it must execute its
/// original test body and return). Returns `true` when the caller is the
/// parent and the child ran and passed the named test exactly once; panics
/// otherwise, so a renamed or missing target can never pass vacuously.
pub(crate) fn run_in_isolated_fixture(
    test_name: &str,
    fixture_variables: &[(&str, &OsStr)],
) -> bool {
    if std::env::var_os(MARKER_VAR).is_some_and(|marker| marker == test_name) {
        return false;
    }

    let test_executable = std::env::current_exe().expect("current test binary must be locatable");
    let mut command = Command::new(test_executable);
    command
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env_clear();
    for key in OS_ESSENTIAL_VARS {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    for (key, value) in fixture_variables {
        command.env(key, value);
    }
    command.env(MARKER_VAR, test_name);

    let output = command
        .output()
        .expect("isolated fixture child process must be spawnable");

    let summary = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && summary.contains("1 passed; 0 failed"),
        "isolated fixture child must run and pass exactly the selected test (anti-vacuum); status: {:?}; child stderr (first 8k chars): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
            .chars()
            .take(8192)
            .collect::<String>()
    );
    true
}

#[test]
fn isolated_child_preserves_temp_directory() {
    let directory = std::env::temp_dir();
    if run_in_isolated_fixture(
        "isolated_env::isolated_child_preserves_temp_directory",
        &[("DBFLUX_EXPECTED_TEMP_DIRECTORY", directory.as_os_str())],
    ) {
        return;
    }

    let expected = std::env::var_os("DBFLUX_EXPECTED_TEMP_DIRECTORY")
        .expect("expected temporary directory must be supplied");
    assert!(
        directory.as_os_str() == expected,
        "isolated child must preserve the caller's temporary directory"
    );
}

#[test]
fn run_in_isolated_fixture_fails_when_exact_target_resolves_to_zero_tests() {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_in_isolated_fixture("isolated_env::no_such_test_target", &[]);
    }));
    assert!(
        result.is_err(),
        "helper must fail when the exact target resolves to zero tests (anti-vacuum)"
    );
}
