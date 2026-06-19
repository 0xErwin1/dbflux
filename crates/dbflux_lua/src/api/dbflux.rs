use crate::engine::LuaRuntimeState;
use dbflux_core::{
    DetachedProcessHandle, OutputEvent, OutputStreamKind, ProcessExecutionError,
    execute_streaming_process,
};
use mlua::{Lua, Result as LuaResult, Table, Value};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

pub fn register_logging_api(lua: &Lua, state: LuaRuntimeState) -> LuaResult<()> {
    let dbflux = ensure_dbflux_table(lua)?;
    let logging = lua.create_table()?;

    logging.set(
        "info",
        log_function(lua, state.clone(), "INFO", |message| {
            log::info!("[lua] {message}");
        })?,
    )?;
    logging.set(
        "warn",
        log_function(lua, state.clone(), "WARN", |message| {
            log::warn!("[lua] {message}");
        })?,
    )?;
    logging.set(
        "error",
        log_function(lua, state, "ERROR", |message| {
            log::error!("[lua] {message}");
        })?,
    )?;

    dbflux.set("log", logging)
}

/// IPC auth-token env-var names that must never be returned to Lua scripts, even
/// when env_read is enabled. These map to dbflux_ipc::{DRIVER_RPC_AUTH_TOKEN_ENV,
/// APP_CONTROL_AUTH_TOKEN_ENV, AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV}.
const BLOCKED_ENV_VARS: &[&str] = &[
    "DBFLUX_DRIVER_IPC_TOKEN",
    "DBFLUX_IPC_TOKEN",
    "DBFLUX_AUTH_PROVIDER_IPC_TOKEN",
];

pub fn register_env_api(lua: &Lua) -> LuaResult<()> {
    let dbflux = ensure_dbflux_table(lua)?;
    let env = lua.create_table()?;

    env.set(
        "get",
        lua.create_function(|_, key: String| {
            if BLOCKED_ENV_VARS
                .iter()
                .any(|blocked| blocked.eq_ignore_ascii_case(&key))
            {
                return Ok(None);
            }
            Ok(std::env::var(key).ok())
        })?,
    )?;

    dbflux.set("env", env)
}

pub fn register_process_api(lua: &Lua, state: LuaRuntimeState) -> LuaResult<()> {
    let dbflux = ensure_dbflux_table(lua)?;
    let process = lua.create_table()?;

    process.set(
        "run",
        lua.create_function(move |lua, options: Table| run_process(lua, &state, options))?,
    )?;

    dbflux.set("process", process)
}

fn ensure_dbflux_table(lua: &Lua) -> LuaResult<Table> {
    let globals = lua.globals();

    if let Ok(existing) = globals.get::<Table>("dbflux") {
        return Ok(existing);
    }

    let dbflux = lua.create_table()?;
    globals.set("dbflux", dbflux.clone())?;
    Ok(dbflux)
}

fn log_function<F>(
    lua: &Lua,
    state: LuaRuntimeState,
    level: &'static str,
    forward: F,
) -> LuaResult<mlua::Function>
where
    F: Fn(&str) + Send + 'static,
{
    lua.create_function(move |_, message: String| {
        append_log(
            &state,
            OutputStreamKind::Log,
            format!("[{level}] {message}"),
        );
        forward(&message);
        Ok(())
    })
}

fn run_process(lua: &Lua, state: &LuaRuntimeState, options: Table) -> LuaResult<Table> {
    let program = read_required_string(&options, "program")?;
    let allowlist = read_required_string(&options, "allowlist")?;
    let args = read_string_list(&options, "args")?;
    let timeout = read_optional_u64(&options, "timeout_ms")?.map(Duration::from_millis);
    let cwd = read_optional_string(&options, "cwd")?;
    let stream_output = read_optional_bool(&options, "stream")?.unwrap_or(false);
    let detached = read_optional_bool(&options, "detached")?.unwrap_or(false);

    if state.cancel_token.is_cancelled()
        || state
            .parent_cancel_token
            .as_ref()
            .is_some_and(|token| token.is_cancelled())
    {
        return Err(mlua::Error::RuntimeError("Lua hook cancelled".to_string()));
    }

    if state
        .hook_timeout
        .is_some_and(|limit| state.hook_started_at.elapsed() >= limit)
    {
        return Err(mlua::Error::RuntimeError("Lua hook timed out".to_string()));
    }

    ensure_program_allowed(&program, &allowlist)?;

    if !detached && timeout.is_none() && state.hook_timeout.is_none() {
        return Err(mlua::Error::RuntimeError(
            "dbflux.process.run requires a timeout_ms when no hook-level timeout is set"
                .to_string(),
        ));
    }

    let mut command = Command::new(&program);
    command.args(&args);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    for var in BLOCKED_ENV_VARS {
        command.env_remove(var);
    }

    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }

    append_log(
        state,
        OutputStreamKind::Log,
        format!(
            "[PROCESS/{allowlist}] {}{}",
            program,
            if args.is_empty() {
                String::new()
            } else {
                format!(" {}", args.join(" "))
            }
        ),
    );

    let mut child = command.spawn().map_err(|error| {
        mlua::Error::RuntimeError(format!("Failed to spawn process '{program}': {error}"))
    })?;

    if detached {
        let Some(detached_sender) = &state.detached else {
            return Err(mlua::Error::RuntimeError(
                "Detached processes are not available in this context".to_string(),
            ));
        };

        let description = format_process_description(&program, &args);

        detached_sender
            .send(DetachedProcessHandle::new(
                child,
                description,
                timeout,
                None,
                None,
            ))
            .map_err(|_| {
                mlua::Error::RuntimeError("Failed to register detached process".to_string())
            })?;

        return process_result_table(lua, None, String::new(), String::new(), false, true);
    }

    let hook_timeout_remaining = state
        .hook_timeout
        .map(|limit| limit.saturating_sub(state.hook_started_at.elapsed()));

    let output = stream_output.then_some(()).and(state.output.as_ref());

    match execute_streaming_process(
        &mut child,
        &state.cancel_token,
        state.parent_cancel_token.as_ref(),
        timeout,
        hook_timeout_remaining,
        output,
    ) {
        Ok(result) => process_result_table(
            lua,
            result.exit_code,
            result.stdout,
            result.stderr,
            result.timed_out,
            false,
        ),
        Err(ProcessExecutionError::Cancelled { .. }) => {
            Err(mlua::Error::RuntimeError("Lua hook cancelled".to_string()))
        }
        Err(ProcessExecutionError::TimedOut { .. }) => {
            Err(mlua::Error::RuntimeError("Lua hook timed out".to_string()))
        }
        Err(ProcessExecutionError::Spawn(error) | ProcessExecutionError::Wait(error)) => Err(
            mlua::Error::RuntimeError(format!("Failed to run process '{program}': {error}")),
        ),
    }
}

fn process_result_table(
    lua: &Lua,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
    detached: bool,
) -> LuaResult<Table> {
    let result = lua.create_table()?;
    result.set("ok", detached || (exit_code == Some(0) && !timed_out))?;
    result.set("detached", detached)?;
    result.set("exit_code", exit_code)?;
    result.set("stdout", stdout)?;
    result.set("stderr", stderr)?;
    result.set("timed_out", timed_out)?;
    Ok(result)
}

fn format_process_description(program: &str, args: &[String]) -> String {
    if args.is_empty() {
        program.to_string()
    } else {
        format!("{} {}", program, args.join(" "))
    }
}

/// Checks whether a program name is path-qualified (contains a path separator or leading `~`).
///
/// This is a footgun guard: it prevents accidental execution of path-qualified names like
/// `/usr/bin/aws` or `../aws` before the allowlist is checked. It is NOT a security
/// isolation boundary — PATH-order manipulation can still substitute a binary, and
/// resolving that requires `which`-style resolution which is out of scope here.
fn is_path_qualified(program: &str) -> bool {
    program.contains('/')
        || program.contains('\\')
        || Path::new(program).components().count() > 1
        || program.starts_with('~')
}

/// Validates that `program` is a bare command name on the named allowlist.
///
/// The allowlist is an ergonomics/footgun guard that prevents typos and unintended
/// execution of programs not expected by the hook author. It is NOT a security
/// isolation boundary: a user controlling PATH can still substitute a different
/// binary under the same name.
fn ensure_program_allowed(program: &str, allowlist: &str) -> LuaResult<()> {
    if is_path_qualified(program) {
        return Err(mlua::Error::RuntimeError(format!(
            "Program '{program}' must be a bare command name (no path separators); \
             allowlist '{allowlist}' resolves via PATH"
        )));
    }

    let Some(allowed_programs) = allowlist_programs(allowlist) else {
        return Err(mlua::Error::RuntimeError(format!(
            "dbflux.process.run allowlist '{allowlist}' is not recognized"
        )));
    };

    if allowed_programs
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(program))
    {
        Ok(())
    } else {
        Err(mlua::Error::RuntimeError(format!(
            "Program '{program}' is not allowed by allowlist '{allowlist}'"
        )))
    }
}

fn allowlist_programs(allowlist: &str) -> Option<&'static [&'static str]> {
    match allowlist {
        "aws_cli" => Some(&["aws", "aws.exe"]),
        "python_cli" => Some(&["python", "python.exe", "python3", "python3.exe"]),
        "ssh_cli" => Some(&["ssh", "ssh.exe"]),
        "cloudflared" => Some(&["cloudflared", "cloudflared.exe"]),
        "gcloud_cli" => Some(&["gcloud", "gcloud.cmd", "gcloud.exe"]),
        "az_cli" => Some(&["az", "az.cmd", "az.exe"]),
        _ => None,
    }
}

fn read_required_string(options: &Table, key: &str) -> LuaResult<String> {
    match options.get::<Value>(key)? {
        Value::String(value) => Ok(value.to_str()?.to_string()),
        Value::Nil => Err(mlua::Error::RuntimeError(format!(
            "dbflux.process.run requires '{key}'"
        ))),
        _ => Err(mlua::Error::RuntimeError(format!(
            "dbflux.process.run field '{key}' must be a string"
        ))),
    }
}

fn read_optional_string(options: &Table, key: &str) -> LuaResult<Option<String>> {
    match options.get::<Value>(key)? {
        Value::String(value) => Ok(Some(value.to_str()?.to_string())),
        Value::Nil => Ok(None),
        _ => Err(mlua::Error::RuntimeError(format!(
            "dbflux.process.run field '{key}' must be a string"
        ))),
    }
}

fn read_optional_u64(options: &Table, key: &str) -> LuaResult<Option<u64>> {
    match options.get::<Value>(key)? {
        Value::Integer(value) if value >= 0 => Ok(Some(value as u64)),
        Value::Nil => Ok(None),
        _ => Err(mlua::Error::RuntimeError(format!(
            "dbflux.process.run field '{key}' must be a non-negative integer"
        ))),
    }
}

fn read_optional_bool(options: &Table, key: &str) -> LuaResult<Option<bool>> {
    match options.get::<Value>(key)? {
        Value::Boolean(value) => Ok(Some(value)),
        Value::Nil => Ok(None),
        _ => Err(mlua::Error::RuntimeError(format!(
            "dbflux.process.run field '{key}' must be a boolean"
        ))),
    }
}

fn read_string_list(options: &Table, key: &str) -> LuaResult<Vec<String>> {
    match options.get::<Value>(key)? {
        Value::Table(table) => table
            .sequence_values::<String>()
            .collect::<Result<Vec<_>, _>>(),
        Value::Nil => Ok(Vec::new()),
        _ => Err(mlua::Error::RuntimeError(format!(
            "dbflux.process.run field '{key}' must be an array of strings"
        ))),
    }
}

fn append_log(state: &LuaRuntimeState, stream: OutputStreamKind, message: String) {
    state
        .log_buffer
        .lock()
        .expect("lua log buffer poisoned")
        .push(message.clone());

    if let Some(output) = &state.output {
        let _ = output.send(OutputEvent::new(stream, format!("{message}\n")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::hook::LuaHookOutcome;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Instant;

    fn test_state(
        hook_timeout: Option<Duration>,
        hook_started_at: Instant,
        output: Option<dbflux_core::OutputSender>,
    ) -> LuaRuntimeState {
        LuaRuntimeState {
            outcome: Arc::new(Mutex::new(LuaHookOutcome::Ok)),
            log_buffer: Arc::new(Mutex::new(Vec::new())),
            output,
            detached: None,
            cancel_token: dbflux_core::CancelToken::new(),
            parent_cancel_token: None,
            hook_started_at,
            hook_timeout,
        }
    }

    fn python_program() -> &'static str {
        if cfg!(target_os = "windows") {
            "python"
        } else {
            "python3"
        }
    }

    fn process_options(lua: &Lua, program: &str) -> LuaResult<Table> {
        let options = lua.create_table()?;
        options.set("program", program)?;
        options.set("allowlist", "python_cli")?;
        Ok(options)
    }

    #[test]
    fn run_process_rejects_expired_hook_timeout_before_spawn() {
        let lua = Lua::new();
        let options = process_options(&lua, python_program()).unwrap();
        let state = test_state(
            Some(Duration::from_millis(1)),
            Instant::now() - Duration::from_secs(1),
            None,
        );

        let error = run_process(&lua, &state, options).unwrap_err().to_string();

        assert!(error.contains("timed out"));
    }

    #[test]
    fn append_log_keeps_buffer_plain_but_streams_newline() {
        let (sender, receiver) = dbflux_core::output_channel();
        let state = test_state(None, Instant::now(), Some(sender));

        append_log(&state, OutputStreamKind::Log, "hello-log".to_string());

        let buffered = state.log_buffer.lock().unwrap().clone();
        let event = receiver.try_recv().unwrap();

        assert_eq!(buffered, vec!["hello-log"]);
        assert_eq!(event.stream, OutputStreamKind::Log);
        assert_eq!(event.text, "hello-log\n");
    }

    #[test]
    fn run_process_cancellation_streams_partial_stdout_and_stderr() {
        let lua = Lua::new();
        let (sender, receiver) = dbflux_core::output_channel();
        let state = test_state(None, Instant::now(), Some(sender));
        let options = process_options(&lua, python_program()).unwrap();
        let args = lua.create_table().unwrap();

        args.set(1, "-c").unwrap();
        args.set(
            2,
            "import sys, time; sys.stdout.write('out'); sys.stdout.flush(); sys.stderr.write('err'); sys.stderr.flush(); time.sleep(5)",
        )
        .unwrap();
        options.set("args", args).unwrap();
        options.set("stream", true).unwrap();
        options.set("timeout_ms", 5000_i64).unwrap();

        let cancel_token = state.cancel_token.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            cancel_token.cancel();
        });

        let error = run_process(&lua, &state, options).unwrap_err().to_string();
        let events: Vec<_> = receiver.try_iter().collect();

        assert!(error.contains("cancelled"));
        assert!(events.iter().any(|event| {
            event.stream == OutputStreamKind::Stdout && event.text.contains("out")
        }));
        assert!(events.iter().any(|event| {
            event.stream == OutputStreamKind::Stderr && event.text.contains("err")
        }));
    }

    // =========================================================================
    // Path-separator rejection
    // =========================================================================

    #[test]
    fn test_absolute_path_rejected() {
        let err = ensure_program_allowed("/usr/bin/aws", "aws_cli").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("bare command name") || msg.contains("path separator"),
            "expected path-separator error, got: {msg}"
        );
    }

    #[test]
    fn test_relative_path_rejected() {
        let err = ensure_program_allowed("../aws", "aws_cli").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("bare command name") || msg.contains("path separator"),
            "expected path-separator error, got: {msg}"
        );
    }

    #[test]
    fn test_bare_allowlisted_passes() {
        assert!(ensure_program_allowed("aws", "aws_cli").is_ok());
    }

    #[test]
    fn test_bare_non_allowlisted_fails_at_allowlist() {
        let err = ensure_program_allowed("malicious_tool", "aws_cli").unwrap_err();
        let msg = err.to_string();
        assert!(
            !msg.contains("bare command name") && !msg.contains("path separator"),
            "expected allowlist error (not path-separator), got: {msg}"
        );
    }

    // =========================================================================
    // Timeout required for non-detached process.run
    // =========================================================================

    #[test]
    fn test_no_timeout_and_no_hook_deadline_rejected() {
        let lua = Lua::new();
        let options = lua.create_table().unwrap();
        options.set("program", "aws").unwrap();
        options.set("allowlist", "aws_cli").unwrap();

        let state = test_state(None, Instant::now(), None);

        let err = run_process(&lua, &state, options).unwrap_err().to_string();
        assert!(
            err.contains("requires a timeout_ms"),
            "expected timeout-required error, got: {err}"
        );
    }

    #[test]
    fn test_timeout_ms_set_accepted() {
        let lua = Lua::new();
        let options = lua.create_table().unwrap();
        options.set("program", "aws").unwrap();
        options.set("allowlist", "aws_cli").unwrap();
        options.set("timeout_ms", 5000_i64).unwrap();

        let state = test_state(None, Instant::now(), None);

        let result = run_process(&lua, &state, options);
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                !msg.contains("requires a timeout_ms"),
                "unexpected timeout-required error: {msg}"
            );
        }
    }

    #[test]
    fn test_hook_deadline_present_accepted() {
        let lua = Lua::new();
        let options = lua.create_table().unwrap();
        options.set("program", "aws").unwrap();
        options.set("allowlist", "aws_cli").unwrap();

        let state = test_state(Some(Duration::from_secs(60)), Instant::now(), None);

        let result = run_process(&lua, &state, options);
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                !msg.contains("requires a timeout_ms"),
                "unexpected timeout-required error when hook timeout is set: {msg}"
            );
        }
    }

    #[test]
    fn test_detached_exempt() {
        let lua = Lua::new();
        let options = lua.create_table().unwrap();
        options.set("program", "aws").unwrap();
        options.set("allowlist", "aws_cli").unwrap();
        options.set("detached", true).unwrap();

        let state = test_state(None, Instant::now(), None);

        let result = run_process(&lua, &state, options);
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                !msg.contains("requires a timeout_ms"),
                "detached run must not require timeout_ms, got: {msg}"
            );
        }
    }

    // =========================================================================
    // IPC token blocklist
    // =========================================================================

    fn make_env_lua() -> Lua {
        let lua = Lua::new();
        register_env_api(&lua).expect("register_env_api failed");
        lua
    }

    #[test]
    fn env_get_blocks_driver_ipc_token_exact_case() {
        // Set the var so it would be readable if not blocked.
        unsafe { std::env::set_var("DBFLUX_DRIVER_IPC_TOKEN", "should-be-blocked") };
        let lua = make_env_lua();
        let value: Option<String> = lua
            .load("return dbflux.env.get('DBFLUX_DRIVER_IPC_TOKEN')")
            .eval()
            .unwrap();
        unsafe { std::env::remove_var("DBFLUX_DRIVER_IPC_TOKEN") };
        assert!(
            value.is_none(),
            "DBFLUX_DRIVER_IPC_TOKEN must be blocked even when env_read is enabled"
        );
    }

    #[test]
    fn env_get_blocks_ipc_token_case_insensitive() {
        unsafe { std::env::set_var("DBFLUX_IPC_TOKEN", "should-be-blocked") };
        let lua = make_env_lua();
        let lower: Option<String> = lua
            .load("return dbflux.env.get('dbflux_ipc_token')")
            .eval()
            .unwrap();
        let upper: Option<String> = lua
            .load("return dbflux.env.get('DBFLUX_IPC_TOKEN')")
            .eval()
            .unwrap();
        unsafe { std::env::remove_var("DBFLUX_IPC_TOKEN") };
        assert!(lower.is_none(), "lowercase key must be blocked");
        assert!(upper.is_none(), "uppercase key must be blocked");
    }

    #[test]
    fn env_get_blocks_auth_provider_ipc_token() {
        unsafe { std::env::set_var("DBFLUX_AUTH_PROVIDER_IPC_TOKEN", "should-be-blocked") };
        let lua = make_env_lua();
        let value: Option<String> = lua
            .load("return dbflux.env.get('DBFLUX_AUTH_PROVIDER_IPC_TOKEN')")
            .eval()
            .unwrap();
        unsafe { std::env::remove_var("DBFLUX_AUTH_PROVIDER_IPC_TOKEN") };
        assert!(
            value.is_none(),
            "DBFLUX_AUTH_PROVIDER_IPC_TOKEN must be blocked"
        );
    }

    #[test]
    fn env_get_allows_unrelated_vars() {
        unsafe { std::env::set_var("DBFLUX_TEST_SAFE_VAR_12345", "visible") };
        let lua = make_env_lua();
        let value: Option<String> = lua
            .load("return dbflux.env.get('DBFLUX_TEST_SAFE_VAR_12345')")
            .eval()
            .unwrap();
        unsafe { std::env::remove_var("DBFLUX_TEST_SAFE_VAR_12345") };
        assert_eq!(value.as_deref(), Some("visible"));
    }
}
