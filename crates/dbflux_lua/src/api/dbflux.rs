use crate::engine::LuaRuntimeState;
use mlua::{Lua, Result as LuaResult, Table, Value};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub fn register_logging_api(lua: &Lua, log_buffer: Arc<Mutex<Vec<String>>>) -> LuaResult<()> {
    let dbflux = ensure_dbflux_table(lua)?;
    let logging = lua.create_table()?;

    logging.set(
        "info",
        log_function(lua, log_buffer.clone(), "INFO", |message| {
            log::info!("[lua] {message}");
        })?,
    )?;
    logging.set(
        "warn",
        log_function(lua, log_buffer.clone(), "WARN", |message| {
            log::warn!("[lua] {message}");
        })?,
    )?;
    logging.set(
        "error",
        log_function(lua, log_buffer, "ERROR", |message| {
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
    log_buffer: Arc<Mutex<Vec<String>>>,
    level: &'static str,
    forward: F,
) -> LuaResult<mlua::Function>
where
    F: Fn(&str) + Send + 'static,
{
    lua.create_function(move |_, message: String| {
        append_log(&log_buffer, format!("[{level}] {message}"));
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

    ensure_program_allowed(&program, &allowlist)?;

    let mut command = Command::new(&program);
    command.args(&args);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }

    append_log(
        &state.log_buffer,
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

    let started_at = Instant::now();

    loop {
        if state.cancel_token.is_cancelled()
            || state
                .parent_cancel_token
                .as_ref()
                .is_some_and(|token| token.is_cancelled())
        {
            let _ = child.kill();
            let _ = child.wait();
            return Err(mlua::Error::RuntimeError("Lua hook cancelled".to_string()));
        }

        if state
            .hook_timeout
            .is_some_and(|limit| state.hook_started_at.elapsed() >= limit)
        {
            let _ = child.kill();
            let _ = child.wait();
            return Err(mlua::Error::RuntimeError("Lua hook timed out".to_string()));
        }

        if timeout.is_some_and(|limit| started_at.elapsed() >= limit) {
            let _ = child.kill();
            let _ = child.wait();
            return process_result_table(lua, None, String::new(), String::new(), true);
        }

        if let Some(status) = child.try_wait().map_err(|error| {
            mlua::Error::RuntimeError(format!("Failed to wait for process '{program}': {error}"))
        })? {
            let output = child.wait_with_output().map_err(|error| {
                mlua::Error::RuntimeError(format!(
                    "Failed to collect process output for '{program}': {error}"
                ))
            })?;

            return process_result_table(
                lua,
                status.code(),
                String::from_utf8_lossy(&output.stdout).into_owned(),
                String::from_utf8_lossy(&output.stderr).into_owned(),
                false,
            );
        }

        thread::sleep(Duration::from_millis(10));
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

fn append_log(log_buffer: &Arc<Mutex<Vec<String>>>, message: String) {
    log_buffer
        .lock()
        .expect("lua log buffer poisoned")
        .push(message);
}
