use crate::api;
use crate::api::hook::LuaHookOutcome;
use dbflux_core::{CancelToken, HookContext, HookPhase, LuaCapabilities, OutputSender};
use mlua::{Lua, LuaOptions, Result as LuaResult, StdLib};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct LuaRuntimeState {
    pub outcome: Arc<Mutex<LuaHookOutcome>>,
    pub log_buffer: Arc<Mutex<Vec<String>>>,
    pub output: Option<OutputSender>,
    pub cancel_token: CancelToken,
    pub parent_cancel_token: Option<CancelToken>,
    pub hook_started_at: Instant,
    pub hook_timeout: Option<Duration>,
}

pub struct LuaVm {
    pub lua: Lua,
    pub state: LuaRuntimeState,
}

pub struct LuaEngine;

impl LuaEngine {
    pub fn create_vm(
        context: &HookContext,
        phase: HookPhase,
        capabilities: &LuaCapabilities,
        cancel_token: CancelToken,
        parent_cancel_token: Option<CancelToken>,
        output: Option<OutputSender>,
        hook_started_at: Instant,
        hook_timeout: Option<Duration>,
    ) -> LuaResult<LuaVm> {
        let stdlib = StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8;
        let lua = Lua::new_with(stdlib, LuaOptions::default())?;

        let state = LuaRuntimeState {
            outcome: Arc::new(Mutex::new(LuaHookOutcome::Ok)),
            log_buffer: Arc::new(Mutex::new(Vec::new())),
            output,
            cancel_token,
            parent_cancel_token,
            hook_started_at,
            hook_timeout,
        };

        api::hook::register_hook_api(&lua, phase, state.outcome.clone())?;

        if capabilities.connection_metadata {
            api::connection::register_connection_api(&lua, context)?;
        }

        if capabilities.logging {
            api::dbflux::register_logging_api(&lua, state.clone())?;
        }

        if capabilities.env_read {
            api::dbflux::register_env_api(&lua)?;
        }

        if capabilities.process_run {
            api::dbflux::register_process_api(&lua, state.clone())?;
        }

        Ok(LuaVm { lua, state })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{CancelToken, HookPhase};
    use uuid::Uuid;

    fn test_context() -> HookContext {
        HookContext {
            profile_id: Uuid::nil(),
            profile_name: "lua-engine-test".to_string(),
            db_kind: "Postgres".to_string(),
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("dbflux".to_string()),
            phase: Some(HookPhase::PostConnect),
        }
    }

    #[test]
    fn registers_hook_phase_and_connection_metadata() {
        let vm = LuaEngine::create_vm(
            &test_context(),
            HookPhase::PostConnect,
            &LuaCapabilities::default(),
            CancelToken::new(),
            None,
            None,
            Instant::now(),
            None,
        )
        .unwrap();

        let phase: String = vm.lua.load("return hook.phase").eval().unwrap();
        let profile_name: String = vm
            .lua
            .load("return connection.profile_name")
            .eval()
            .unwrap();
        let host: String = vm.lua.load("return connection.host").eval().unwrap();

        assert_eq!(phase, "post_connect");
        assert_eq!(profile_name, "lua-engine-test");
        assert_eq!(host, "localhost");
    }

    #[test]
    fn does_not_load_unsafe_libraries() {
        let vm = LuaEngine::create_vm(
            &test_context(),
            HookPhase::PreConnect,
            &LuaCapabilities::default(),
            CancelToken::new(),
            None,
            None,
            Instant::now(),
            None,
        )
        .unwrap();

        let io_is_nil: bool = vm.lua.load("return io == nil").eval().unwrap();
        let os_is_nil: bool = vm.lua.load("return os == nil").eval().unwrap();
        let debug_is_nil: bool = vm.lua.load("return debug == nil").eval().unwrap();
        let package_is_nil: bool = vm.lua.load("return package == nil").eval().unwrap();

        assert!(io_is_nil);
        assert!(os_is_nil);
        assert!(debug_is_nil);
        assert!(package_is_nil);
    }

    #[test]
    fn capabilities_hide_optional_apis() {
        let vm = LuaEngine::create_vm(
            &test_context(),
            HookPhase::PreConnect,
            &LuaCapabilities {
                logging: false,
                env_read: false,
                connection_metadata: false,
                process_run: false,
            },
            CancelToken::new(),
            None,
            None,
            Instant::now(),
            None,
        )
        .unwrap();

        let connection_is_nil: bool = vm.lua.load("return connection == nil").eval().unwrap();
        let logging_is_nil: bool = vm
            .lua
            .load("return dbflux == nil or dbflux.log == nil")
            .eval()
            .unwrap();
        let env_is_nil: bool = vm
            .lua
            .load("return dbflux == nil or dbflux.env == nil")
            .eval()
            .unwrap();

        assert!(connection_is_nil);
        assert!(logging_is_nil);
        assert!(env_is_nil);
    }

    #[test]
    fn logging_and_env_api_are_available_when_enabled() {
        let vm = LuaEngine::create_vm(
            &test_context(),
            HookPhase::PreConnect,
            &LuaCapabilities::default(),
            CancelToken::new(),
            None,
            None,
            Instant::now(),
            None,
        )
        .unwrap();

        let has_logging: bool = vm
            .lua
            .load("return dbflux ~= nil and dbflux.log ~= nil")
            .eval()
            .unwrap();
        let has_env: bool = vm
            .lua
            .load("return dbflux ~= nil and dbflux.env ~= nil")
            .eval()
            .unwrap();
        let path_value: Option<String> =
            vm.lua.load("return dbflux.env.get('PATH')").eval().unwrap();

        assert!(has_logging);
        assert!(has_env);
        assert!(path_value.is_some());
    }

    #[test]
    fn process_api_is_hidden_when_capability_is_disabled() {
        let vm = LuaEngine::create_vm(
            &test_context(),
            HookPhase::PreConnect,
            &LuaCapabilities::default(),
            CancelToken::new(),
            None,
            None,
            Instant::now(),
            None,
        )
        .unwrap();

        let has_process: bool = vm
            .lua
            .load("return dbflux == nil or dbflux.process ~= nil")
            .eval()
            .unwrap();

        assert!(!has_process);
    }

    #[test]
    fn process_api_is_available_when_capability_is_enabled() {
        let vm = LuaEngine::create_vm(
            &test_context(),
            HookPhase::PreConnect,
            &LuaCapabilities {
                process_run: true,
                ..LuaCapabilities::default()
            },
            CancelToken::new(),
            None,
            None,
            Instant::now(),
            None,
        )
        .unwrap();

        let has_process: bool = vm
            .lua
            .load("return dbflux ~= nil and dbflux.process ~= nil and dbflux.process.run ~= nil")
            .eval()
            .unwrap();

        assert!(has_process);
    }
}
