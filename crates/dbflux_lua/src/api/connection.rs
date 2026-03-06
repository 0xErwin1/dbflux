use dbflux_core::HookContext;
use mlua::{Lua, Result as LuaResult, Table};

pub fn register_connection_api(lua: &Lua, context: &HookContext) -> LuaResult<()> {
    let connection = lua.create_table()?;
    connection.set("profile_id", context.profile_id.to_string())?;
    connection.set("profile_name", context.profile_name.clone())?;
    connection.set("db_kind", context.db_kind.clone())?;
    connection.set("host", context.host.clone())?;
    connection.set("port", context.port)?;
    connection.set("database", context.database.clone())?;

    set_global_table(lua, "connection", connection)
}

fn set_global_table(lua: &Lua, name: &str, table: Table) -> LuaResult<()> {
    lua.globals().set(name, table)
}
