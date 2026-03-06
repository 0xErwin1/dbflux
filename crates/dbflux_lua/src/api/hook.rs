use dbflux_core::HookPhase;
use mlua::{Lua, Result as LuaResult, Table};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub enum LuaHookOutcome {
    #[default]
    Ok,
    Warn(String),
    Fail(String),
}

pub fn register_hook_api(
    lua: &Lua,
    phase: HookPhase,
    outcome: Arc<Mutex<LuaHookOutcome>>,
) -> LuaResult<()> {
    let hook = lua.create_table()?;
    hook.set("phase", phase_name(phase))?;

    hook.set(
        "ok",
        lua.create_function({
            let outcome = outcome.clone();
            move |_, ()| {
                *outcome.lock().expect("lua hook outcome poisoned") = LuaHookOutcome::Ok;
                Ok(())
            }
        })?,
    )?;

    hook.set(
        "warn",
        lua.create_function({
            let outcome = outcome.clone();
            move |_, message: String| {
                *outcome.lock().expect("lua hook outcome poisoned") = LuaHookOutcome::Warn(message);
                Ok(())
            }
        })?,
    )?;

    hook.set(
        "fail",
        lua.create_function(move |_, message: String| {
            *outcome.lock().expect("lua hook outcome poisoned") = LuaHookOutcome::Fail(message);
            Ok(())
        })?,
    )?;

    set_global_table(lua, "hook", hook)
}

fn phase_name(phase: HookPhase) -> &'static str {
    match phase {
        HookPhase::PreConnect => "pre_connect",
        HookPhase::PostConnect => "post_connect",
        HookPhase::PreDisconnect => "pre_disconnect",
        HookPhase::PostDisconnect => "post_disconnect",
    }
}

fn set_global_table(lua: &Lua, name: &str, table: Table) -> LuaResult<()> {
    lua.globals().set(name, table)
}
