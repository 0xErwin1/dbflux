use dbflux_core::{
    CancelToken, ConnectionHook, HookContext, HookExecutor, HookKind, HookResult, ProcessExecutor,
};

#[derive(Clone)]
pub struct CompositeExecutor {
    process: ProcessExecutor,
    #[cfg(feature = "lua")]
    lua: dbflux_lua::LuaExecutor,
}

impl CompositeExecutor {
    pub fn new() -> Self {
        Self {
            process: ProcessExecutor,
            #[cfg(feature = "lua")]
            lua: dbflux_lua::LuaExecutor::new(),
        }
    }
}

impl Default for CompositeExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl HookExecutor for CompositeExecutor {
    fn execute_hook(
        &self,
        hook: &ConnectionHook,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
    ) -> Result<HookResult, String> {
        match &hook.kind {
            HookKind::Command { .. } | HookKind::Script { .. } => {
                self.process
                    .execute_hook(hook, context, cancel_token, parent_cancel_token)
            }
            #[cfg(feature = "lua")]
            HookKind::Lua { .. } => {
                self.lua
                    .execute_hook(hook, context, cancel_token, parent_cancel_token)
            }
            #[cfg(not(feature = "lua"))]
            HookKind::Lua { .. } => {
                Err("Lua hooks require the 'lua' feature to be enabled".to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        HookFailureMode, HookPhase, HookPhaseOutcome, HookRunner, LuaCapabilities, ScriptSource,
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    fn test_context() -> HookContext {
        HookContext {
            profile_id: Uuid::nil(),
            profile_name: "composite-test".to_string(),
            db_kind: "Postgres".to_string(),
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("dbflux".to_string()),
            phase: Some(HookPhase::PreConnect),
        }
    }

    fn command_hook() -> ConnectionHook {
        let (command, args) = if cfg!(target_os = "windows") {
            (
                "cmd".to_string(),
                vec!["/C".to_string(), "echo command-ok".to_string()],
            )
        } else {
            (
                "sh".to_string(),
                vec!["-c".to_string(), "printf command-ok".to_string()],
            )
        };

        ConnectionHook {
            enabled: true,
            kind: HookKind::Command { command, args },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    fn lua_hook() -> ConnectionHook {
        ConnectionHook {
            enabled: true,
            kind: HookKind::Lua {
                source: ScriptSource::Inline {
                    content: "dbflux.log.info('lua-ok')".to_string(),
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
    fn composite_executor_routes_mixed_hook_types() {
        let hooks = vec![command_hook(), lua_hook()];
        let executor = CompositeExecutor::new();

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
            &executor,
        );

        match outcome {
            HookPhaseOutcome::Success { executions }
            | HookPhaseOutcome::CompletedWithWarnings { executions, .. } => {
                assert_eq!(executions.len(), 2);
                assert!(executions[0]
                    .result
                    .as_ref()
                    .is_ok_and(|result| result.stdout.contains("command-ok")));
                assert!(executions[1]
                    .result
                    .as_ref()
                    .is_ok_and(|result| result.stdout.contains("lua-ok")));
            }
            HookPhaseOutcome::Aborted { error, .. } => panic!("unexpected abort: {error}"),
        }
    }
}
