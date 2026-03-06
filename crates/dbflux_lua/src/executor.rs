use crate::api::hook::LuaHookOutcome;
use crate::engine::LuaEngine;
use dbflux_core::{
    CancelToken, ConnectionHook, HookContext, HookExecutor, HookKind, HookPhase, HookResult,
    OutputSender,
};
use mlua::{Error as LuaError, HookTriggers, VmState};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub struct LuaExecutor;

impl LuaExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LuaExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl HookExecutor for LuaExecutor {
    fn execute_hook(
        &self,
        hook: &ConnectionHook,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
        output: Option<&OutputSender>,
    ) -> Result<HookResult, String> {
        let HookKind::Lua {
            source,
            capabilities,
        } = &hook.kind
        else {
            return Err("LuaExecutor received non-Lua hook".to_string());
        };

        let script_content = match source {
            dbflux_core::ScriptSource::Inline { content } => content.clone(),
            dbflux_core::ScriptSource::File { path } => {
                std::fs::read_to_string(path).map_err(|error| {
                    format!("Failed to read Lua script {}: {error}", path.display())
                })?
            }
        };

        let phase = context.phase.unwrap_or(HookPhase::PreConnect);
        let start = Instant::now();
        let timeout = hook.timeout_ms.map(Duration::from_millis);
        let vm = LuaEngine::create_vm(
            context,
            phase,
            capabilities,
            cancel_token.clone(),
            parent_cancel_token.cloned(),
            output.cloned(),
            start,
            timeout,
        )
        .map_err(|error| format!("Failed to create Lua VM: {error}"))?;
        let cancel = cancel_token.clone();
        let parent = parent_cancel_token.cloned();

        vm.lua.set_hook(
            HookTriggers::new().every_nth_instruction(1_000),
            move |_, _| {
                if cancel.is_cancelled() || parent.as_ref().is_some_and(CancelToken::is_cancelled) {
                    return Err(LuaError::RuntimeError("Lua hook cancelled".to_string()));
                }

                if timeout.is_some_and(|max| start.elapsed() > max) {
                    return Err(LuaError::RuntimeError("Lua hook timed out".to_string()));
                }

                Ok(VmState::Continue)
            },
        );

        let exec_result = vm.lua.load(&script_content).exec();
        vm.lua.remove_hook();

        let stdout = {
            let buffer = vm.state.log_buffer.lock().expect("lua log buffer poisoned");
            buffer.join("\n")
        };
        let outcome = vm
            .state
            .outcome
            .lock()
            .expect("lua hook outcome poisoned")
            .clone();

        match exec_result {
            Ok(()) => Ok(map_outcome(stdout, outcome)),
            Err(error) if error_has_message(&error, "Lua hook cancelled") => {
                if stdout.is_empty() {
                    Err(format!("Hook '{}' cancelled", hook.display_command()))
                } else {
                    Err(format!(
                        "Hook '{}' cancelled\n{}",
                        hook.display_command(),
                        stdout
                    ))
                }
            }
            Err(error) if error_has_message(&error, "Lua hook timed out") => Ok(HookResult {
                exit_code: None,
                stdout,
                stderr: String::new(),
                timed_out: true,
                warnings: Vec::new(),
            }),
            Err(error) => Ok(HookResult {
                exit_code: Some(1),
                stdout,
                stderr: error.to_string(),
                timed_out: false,
                warnings: Vec::new(),
            }),
        }
    }
}

fn error_has_message(error: &LuaError, expected: &str) -> bool {
    match error {
        LuaError::RuntimeError(message) => message == expected,
        LuaError::CallbackError { cause, .. } => error_has_message(cause.as_ref(), expected),
        LuaError::WithContext { cause, .. } => error_has_message(cause.as_ref(), expected),
        _ => false,
    }
}

fn map_outcome(stdout: String, outcome: LuaHookOutcome) -> HookResult {
    match outcome {
        LuaHookOutcome::Ok => HookResult {
            exit_code: Some(0),
            stdout,
            stderr: String::new(),
            timed_out: false,
            warnings: Vec::new(),
        },
        LuaHookOutcome::Warn(message) => HookResult {
            exit_code: Some(0),
            stdout,
            stderr: String::new(),
            timed_out: false,
            warnings: vec![message],
        },
        LuaHookOutcome::Fail(message) => HookResult {
            exit_code: Some(1),
            stdout,
            stderr: message,
            timed_out: false,
            warnings: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        HookFailureMode, LuaCapabilities, OutputStreamKind, ScriptSource, output_channel,
    };
    use std::collections::HashMap;
    use std::io::Write;
    use uuid::Uuid;

    fn test_context() -> HookContext {
        HookContext {
            profile_id: Uuid::nil(),
            profile_name: "lua-test".to_string(),
            db_kind: "Postgres".to_string(),
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("dbflux".to_string()),
            phase: Some(HookPhase::PreConnect),
        }
    }

    fn lua_hook(content: &str) -> ConnectionHook {
        lua_hook_with_capabilities(content, LuaCapabilities::default())
    }

    fn lua_hook_with_capabilities(content: &str, capabilities: LuaCapabilities) -> ConnectionHook {
        ConnectionHook {
            enabled: true,
            kind: HookKind::Lua {
                source: ScriptSource::Inline {
                    content: content.to_string(),
                },
                capabilities,
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    fn file_lua_hook(content: &str) -> ConnectionHook {
        let mut file = tempfile::Builder::new().suffix(".lua").tempfile().unwrap();
        file.write_all(content.as_bytes()).unwrap();

        ConnectionHook {
            enabled: true,
            kind: HookKind::Lua {
                source: ScriptSource::File {
                    path: file.into_temp_path().keep().unwrap(),
                },
                capabilities: LuaCapabilities::default(),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    #[test]
    fn executes_lua_hook_successfully() {
        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook("dbflux.log.info('hello')"),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert!(result.stdout.contains("hello"));
    }

    #[test]
    fn executes_lua_hook_warning() {
        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook("hook.warn('watch out')"),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.warnings, vec!["watch out"]);
    }

    #[test]
    fn executes_lua_hook_failure() {
        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook("hook.fail('boom')"),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(1));
        assert_eq!(result.stderr, "boom");
    }

    #[test]
    fn executes_file_backed_lua_hook() {
        let result = LuaExecutor::new()
            .execute_hook(
                &file_lua_hook("dbflux.log.info('from-file')"),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert!(result.stdout.contains("from-file"));
    }

    #[test]
    fn lua_runtime_error_becomes_failed_result() {
        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook("local x = nil; return x.missing"),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(1));
        assert!(result.stderr.contains("nil"));
    }

    #[test]
    fn lua_hook_timeout_is_reported() {
        let mut hook = lua_hook("while true do end");
        hook.timeout_ms = Some(10);

        let result = LuaExecutor::new()
            .execute_hook(&hook, &test_context(), &CancelToken::new(), None, None)
            .unwrap();

        assert!(result.timed_out);
        assert_eq!(result.exit_code, None);
    }

    #[test]
    fn lua_hook_cancellation_returns_error() {
        let token = CancelToken::new();
        token.cancel();

        let result = LuaExecutor::new().execute_hook(
            &lua_hook("while true do end"),
            &test_context(),
            &token,
            None,
            None,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cancelled"));
    }

    #[test]
    fn controlled_process_run_executes_when_enabled() {
        let python = if cfg!(target_os = "windows") {
            "python"
        } else {
            "python3"
        };

        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook_with_capabilities(
                    &format!(
                        "local result = dbflux.process.run({{ program = '{python}', allowlist = 'python_cli', args = {{'-c', 'print(\"hello-process\")'}} }})\nif not result.ok then hook.fail(result.stderr) end"
                    ),
                    LuaCapabilities {
                        process_run: true,
                        ..LuaCapabilities::default()
                    },
                ),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert!(result.stdout.contains("[PROCESS/python_cli]"));
    }

    #[test]
    fn controlled_process_run_is_unavailable_without_capability() {
        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook(
                    "return dbflux.process.run({ program = 'python3', allowlist = 'python_cli' })",
                ),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(1));
        assert!(result.stderr.contains("process"));
    }

    #[test]
    fn controlled_process_run_rejects_unknown_allowlist() {
        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook_with_capabilities(
                    "return dbflux.process.run({ program = 'python3', allowlist = 'nope' })",
                    LuaCapabilities {
                        process_run: true,
                        ..LuaCapabilities::default()
                    },
                ),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(1));
        assert!(result.stderr.contains("allowlist 'nope'"));
    }

    #[test]
    fn controlled_process_run_rejects_program_outside_allowlist() {
        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook_with_capabilities(
                    "return dbflux.process.run({ program = 'python3', allowlist = 'aws_cli' })",
                    LuaCapabilities {
                        process_run: true,
                        ..LuaCapabilities::default()
                    },
                ),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(1));
        assert!(result.stderr.contains("not allowed"));
    }

    #[test]
    fn controlled_process_run_reports_process_timeout() {
        let python = if cfg!(target_os = "windows") {
            "python"
        } else {
            "python3"
        };

        let result = LuaExecutor::new()
            .execute_hook(
                &lua_hook_with_capabilities(
                    &format!(
                        "local result = dbflux.process.run({{ program = '{python}', allowlist = 'python_cli', timeout_ms = 10, args = {{'-c', 'import time; time.sleep(10)'}} }})\nif not result.timed_out then hook.fail('expected process timeout') end"
                    ),
                    LuaCapabilities {
                        process_run: true,
                        ..LuaCapabilities::default()
                    },
                ),
                &test_context(),
                &CancelToken::new(),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
    }

    #[test]
    fn lua_executor_streams_logs_and_process_output() {
        let python = if cfg!(target_os = "windows") {
            "python"
        } else {
            "python3"
        };

        let hook = lua_hook_with_capabilities(
            &format!(
                "dbflux.log.info('hello-log')\nlocal result = dbflux.process.run({{ program = '{python}', allowlist = 'python_cli', stream = true, args = {{'-c', 'print(\"hello-stream\")'}} }})\nif not result.ok then hook.fail(result.stderr) end"
            ),
            LuaCapabilities {
                process_run: true,
                ..LuaCapabilities::default()
            },
        );

        let (sender, receiver) = output_channel();
        let result = LuaExecutor::new()
            .execute_hook(
                &hook,
                &test_context(),
                &CancelToken::new(),
                None,
                Some(&sender),
            )
            .unwrap();
        drop(sender);

        let events: Vec<_> = receiver.try_iter().collect();

        assert!(result.stdout.contains("hello-log"));
        assert!(result.stdout.contains("[PROCESS/python_cli]"));
        assert!(result.stdout.contains("hello-stream"));
        assert!(events.iter().any(|event| {
            event.stream == OutputStreamKind::Log && event.text.contains("hello-log")
        }));
        assert!(events.iter().any(|event| {
            event.stream == OutputStreamKind::Stdout && event.text.contains("hello-stream")
        }));
    }

    #[test]
    fn lua_executor_only_streams_process_output_when_requested() {
        let python = if cfg!(target_os = "windows") {
            "python"
        } else {
            "python3"
        };

        let hook = lua_hook_with_capabilities(
            &format!(
                "dbflux.log.info('hello-log')\nlocal result = dbflux.process.run({{ program = '{python}', allowlist = 'python_cli', args = {{'-c', 'print(\"hello-buffered\")'}} }})\nif not result.ok then hook.fail(result.stderr) end"
            ),
            LuaCapabilities {
                process_run: true,
                ..LuaCapabilities::default()
            },
        );

        let (sender, receiver) = output_channel();
        let result = LuaExecutor::new()
            .execute_hook(
                &hook,
                &test_context(),
                &CancelToken::new(),
                None,
                Some(&sender),
            )
            .unwrap();
        drop(sender);

        let events: Vec<_> = receiver.try_iter().collect();

        assert!(result.stdout.contains("hello-log"));
        assert!(result.stdout.contains("hello-buffered"));
        assert!(events.iter().any(|event| {
            event.stream == OutputStreamKind::Log && event.text.contains("hello-log")
        }));
        assert!(!events.iter().any(|event| {
            event.stream == OutputStreamKind::Stdout && event.text.contains("hello-buffered")
        }));
    }
}
