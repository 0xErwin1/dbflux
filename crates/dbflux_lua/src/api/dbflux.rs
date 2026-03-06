use crate::engine::LuaRuntimeState;
use dbflux_core::{
    OutputEvent, OutputStreamKind, ProcessExecutionError, execute_streaming_process,
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

pub fn register_env_api(lua: &Lua) -> LuaResult<()> {
    let dbflux = ensure_dbflux_table(lua)?;
    let env = lua.create_table()?;

    env.set(
        "get",
        lua.create_function(|_, key: String| Ok(std::env::var(key).ok()))?,
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

    ensure_program_allowed(&program, &allowlist)?;

    let mut command = Command::new(&program);
    command.args(&args);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

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

    let hook_timeout_remaining = state.hook_timeout.and_then(|limit| {
        let elapsed = state.hook_started_at.elapsed();
        (elapsed < limit).then_some(limit - elapsed)
    });

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
) -> LuaResult<Table> {
    let result = lua.create_table()?;
    result.set("ok", exit_code == Some(0) && !timed_out)?;
    result.set("exit_code", exit_code)?;
    result.set("stdout", stdout)?;
    result.set("stderr", stderr)?;
    result.set("timed_out", timed_out)?;
    Ok(result)
}

fn ensure_program_allowed(program: &str, allowlist: &str) -> LuaResult<()> {
    let Some(allowed_programs) = allowlist_programs(allowlist) else {
        return Err(mlua::Error::RuntimeError(format!(
            "dbflux.process.run allowlist '{allowlist}' is not recognized"
        )));
    };

    let program_name = Path::new(program)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(program);

    if allowed_programs
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(program_name))
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
        let _ = output.send(OutputEvent::new(stream, message));
    }
}
