//! Regression coverage for the process-global IPC auth token store.
//!
//! Tests that observe `init_process_auth_tokens()` side effects on the process
//! environment or on spawned children run in a hermetic re-exec child (the
//! approach of `dbflux_core::isolated_env`): the token store must be exercised
//! in a process whose environment is controlled by the test, never in the
//! multithreaded harness process. Token values are never printed.
//!
//! The fixtures redirect every base variable `dirs::data_dir()` consults —
//! `XDG_DATA_HOME` on Linux, `HOME` on macOS and `APPDATA` on Windows — so the
//! token file is never written into the developer's real data directory.

use std::ffi::OsStr;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

// Only the Unix-only provider-spawn test needs these; importing them
// unconditionally would leave them unused on Windows, where that test is
// compiled out.
#[cfg(unix)]
use std::time::Duration;

use dbflux_ipc::{
    APP_CONTROL_AUTH_TOKEN_ENV, AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV, DRIVER_RPC_AUTH_TOKEN_ENV,
    init_process_auth_tokens, process_auth_token, read_app_control_token,
};
#[cfg(unix)]
use dbflux_ipc::{IpcServiceLaunchConfig, RpcAuthProvider};

const MARKER_VAR: &str = "DBFLUX_IPC_AUTH_TOKEN_ISOLATED_FIXTURE";

/// OS variables required to locate and run executables on each platform.
const OS_ESSENTIAL_VARS: &[&str] = &["PATH", "SystemRoot", "SystemDrive", "TEMP", "TMP", "TMPDIR"];

/// Runs `test_name` in an isolated child process with a cleared environment.
///
/// Returns `Ok(false)` when the caller is the isolated child (it must execute
/// its original test body and return). Returns `Ok(true)` when the caller is
/// the parent and the child ran and passed the named test exactly once;
/// `Err` otherwise, so a renamed or missing target can never pass vacuously.
fn run_in_isolated_fixture(
    test_name: &str,
    fixture_variables: &[(&str, &OsStr)],
) -> Result<bool, String> {
    if std::env::var_os(MARKER_VAR).is_some_and(|marker| marker == test_name) {
        return Ok(false);
    }

    let test_executable = std::env::current_exe()
        .map_err(|error| format!("current test binary must be locatable: {error}"))?;
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
        .map_err(|error| format!("isolated fixture child process must be spawnable: {error}"))?;

    let summary = String::from_utf8_lossy(&output.stdout);
    if !(output.status.success() && summary.contains("1 passed; 0 failed")) {
        return Err(format!(
            "isolated fixture child must run and pass exactly the selected test (anti-vacuum); status: {:?}; child stderr (first 8k chars): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
                .chars()
                .take(8192)
                .collect::<String>()
        ));
    }
    Ok(true)
}

/// Unique scratch data directory for a fixture child, so the token file write
/// never touches user data.
fn fixture_data_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "dbflux-ipc-token-fixture-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ))
}

#[test]
fn init_process_auth_tokens_leaves_process_environment_untouched() {
    let data_dir = fixture_data_dir("env-untouched");

    if run_in_isolated_fixture(
        "init_process_auth_tokens_leaves_process_environment_untouched",
        &[
            ("XDG_DATA_HOME", data_dir.as_os_str()),
            ("HOME", data_dir.as_os_str()),
            ("APPDATA", data_dir.as_os_str()),
        ],
    )
    .expect("isolated fixture must pass")
    {
        return;
    }

    // Child side: the environment is exactly what the parent provisioned.
    let token = init_process_auth_tokens().expect("token initialization must succeed");

    for variable in [
        APP_CONTROL_AUTH_TOKEN_ENV,
        DRIVER_RPC_AUTH_TOKEN_ENV,
        AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV,
    ] {
        assert!(
            std::env::var_os(variable).is_none(),
            "{variable} must be absent from the process environment after init"
        );
    }

    assert!(
        process_auth_token() == Some(token.as_str()),
        "accessor must return the initialized token"
    );
}

/// A managed auth-provider launch whose "host" is a shell command that records
/// the token it received into a file, so the delivery path is observable
/// without running a real provider. Unix only: it relies on `sh`.
#[cfg(unix)]
#[test]
fn spawned_auth_provider_host_receives_store_token_explicitly() {
    let data_dir = fixture_data_dir("authprov-spawn");

    if run_in_isolated_fixture(
        "spawned_auth_provider_host_receives_store_token_explicitly",
        &[
            ("XDG_DATA_HOME", data_dir.as_os_str()),
            ("HOME", data_dir.as_os_str()),
            ("APPDATA", data_dir.as_os_str()),
        ],
    )
    .expect("isolated fixture must pass")
    {
        return;
    }

    // Child side: the store holds the token; the environment does not.
    let token = init_process_auth_tokens().expect("token initialization must succeed");

    // The scratch dir is whatever the parent provisioned via the fixture.
    let data_dir = std::env::var_os("XDG_DATA_HOME")
        .map(std::path::PathBuf::from)
        .expect("fixture must provide XDG_DATA_HOME");

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    let socket_id = format!("authprov-token-{}-{}", std::process::id(), nanos);
    let token_out = data_dir.join("auth_provider_token");

    let launch = IpcServiceLaunchConfig {
        program: "sh".to_string(),
        args: vec![
            "-c".to_string(),
            "printf '%s' \"$DBFLUX_AUTH_PROVIDER_IPC_TOKEN\" > \"$TOKEN_OUT_FILE\"".to_string(),
        ],
        env: vec![(
            "TOKEN_OUT_FILE".to_string(),
            token_out.display().to_string(),
        )],
        startup_timeout: Duration::from_millis(500),
    };

    // The probe fails by design: the shell host exits without serving a
    // socket. Its error is irrelevant; the delivered token is the assertion.
    let _probe_error = RpcAuthProvider::probe(&socket_id, Some(launch));

    let delivered = std::fs::read_to_string(&token_out)
        .expect("spawned auth-provider host must write the delivered token");
    assert!(
        delivered == token,
        "spawned auth-provider host must receive the store token explicitly \
         (delivered length {}, expected length {})",
        delivered.len(),
        token.len()
    );
}

/// Repeated initialization must stay consistent: the token in force is the one
/// returned and the one written to the app-control file. If a second call
/// generated a fresh token while the store kept the first, the parent and a
/// spawned child could authenticate with different values.
#[test]
fn init_process_auth_tokens_is_idempotent() {
    let data_dir = fixture_data_dir("idempotent");

    if run_in_isolated_fixture(
        "init_process_auth_tokens_is_idempotent",
        &[
            ("XDG_DATA_HOME", data_dir.as_os_str()),
            ("HOME", data_dir.as_os_str()),
            ("APPDATA", data_dir.as_os_str()),
        ],
    )
    .expect("isolated fixture must pass")
    {
        return;
    }

    // Whatever base variable this platform uses, the resolved token file must
    // land inside the fixture directory. Without the HOME and APPDATA entries
    // alongside XDG_DATA_HOME this assertion fails on macOS and Windows, where
    // the fixture would otherwise write the developer's real token file.
    let fixture_dir = std::env::var_os("XDG_DATA_HOME")
        .map(std::path::PathBuf::from)
        .expect("fixture must provide XDG_DATA_HOME");
    let token_path = dbflux_ipc::app_control_token_path().expect("token path must resolve");
    assert!(
        token_path.starts_with(&fixture_dir),
        "the token file must land under the fixture directory, not the real data \
         directory; resolved {}",
        token_path.display()
    );

    let first = init_process_auth_tokens().expect("first init must succeed");
    let second = init_process_auth_tokens().expect("second init must succeed");

    assert_eq!(
        first, second,
        "a repeated init must return the token already in force"
    );
    assert_eq!(
        process_auth_token(),
        Some(first.as_str()),
        "the store must keep the token the caller received"
    );
    assert_eq!(
        read_app_control_token().expect("token file must be readable"),
        first,
        "the written file must match the token in force"
    );
}
