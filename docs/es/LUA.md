# El runtime de Lua embebido

El crate `dbflux_lua` es el runtime de Lua 5.4 sandboxed de DBFlux para los hooks de conexión. Este documento describe la arquitectura del crate, la API de Lua expuesta a los scripts de hooks, el modelo de sandbox y timeout, y cómo el runtime se integra en la aplicación.

---

## Qué hace este crate

`dbflux_lua` permite a los usuarios escribir scripts de Lua que se ejecutan durante los eventos del ciclo de vida de la conexión (pre-connect, post-connect, pre-disconnect, post-disconnect). Los hooks son de propósito general: pueden impulsar flujos de login SSO, configuración de entorno, audit logging, o disparar herramientas externas antes/después de que se abra una conexión.

El crate expone exactamente un tipo público: `LuaExecutor`. Todo lo demás — la fábrica de VMs, los módulos de API, el estado compartido — es interno al crate. Desde afuera, llamas a `executor.execute_hook(hook, context, cancel_token, parent_cancel_token, output, detached)` y obtienes un `HookResult`. El argumento final `detached: Option<&DetachedProcessSender>` permite que el executor entregue procesos detached de larga duración de vuelta al caller.

---

## Visión general de la arquitectura

```mermaid
flowchart TD
    subgraph APP["dbflux (app crate)"]
        COMPOSITE["CompositeExecutor"]
        PROCESS["ProcessExecutor<br/>commands, scripts"]
        LUAEXEC["LuaExecutor<br/>Lua hooks — feature = lua"]
        COMPOSITE --> PROCESS
        COMPOSITE --> LUAEXEC
    end

    subgraph LUA["dbflux_lua"]
        EXEC["LuaExecutor (zero-sized)"]
        VM["fresh LuaVm per call"]
        MLUA["Lua 5.4 VM (mlua)"]
        STATE["LuaRuntimeState (shared)"]
        HOOKI["instruction hook (1000)"]
        API["API modules<br/>hook.* always<br/>connection.* capability<br/>dbflux.log.* capability<br/>dbflux.env.* capability<br/>dbflux.process.* capability + gate"]
        EXEC --> VM
        VM --> MLUA
        VM --> STATE
        VM --> HOOKI
        EXEC --> API
    end

    subgraph CORE["dbflux_core"]
        TRAIT["HookExecutor trait"]
        TYPES["ConnectionHook, HookKind::Lua<br/>LuaCapabilities, HookContext<br/>HookResult, CancelToken"]
    end

    LUAEXEC -->|implements HookExecutor| EXEC
    EXEC -->|types + traits| TRAIT
```

El principio de diseño clave: **se crea una VM de Lua nueva para cada ejecución de hook**. Sin pooling de VMs, sin estado que se filtre entre ejecuciones. Esto hace que el sandbox sea trivialmente seguro — incluso si un script de alguna forma corrompe el estado de la VM, esta se descarta después de la ejecución.

---

## Dependencias

| Dependencia   | Versión   | Propósito                                                                                                |
| ------------- | --------- | ------------------------------------------------------------------------------------------------------- |
| `mlua`        | 0.10      | Bindings de Lua 5.4. Features: `lua54`, `send` (hace `Lua: Send`), `vendored` (compila Lua desde source) |
| `dbflux_core` | workspace | Traits (`HookExecutor`), tipos (`ConnectionHook`, `HookContext`, etc.)                                   |
| `log`         | 0.4       | Logging desde el lado de Rust para callbacks de Lua                                                      |

El feature `vendored` es importante — significa que no se requiere una instalación de Lua a nivel de sistema. El intérprete de Lua 5.4 se compila desde código fuente en C y se enlaza estáticamente. Esto elimina una dependencia de deployment pero agrega ~200KB al binario.

---

## El sandbox

### Qué se carga

Solo cuatro librerías estándar de Lua:

```rust
let stdlib = StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8;
let lua = Lua::new_with(stdlib, LuaOptions::default())?;
```

Esto le da a los scripts acceso a:

- **table**: `table.insert`, `table.remove`, `table.sort`, `table.concat`, `table.pack`, `table.unpack`
- **string**: `string.format`, `string.find`, `string.gsub`, `string.sub`, `string.len`, `string.match`, `string.rep`, pattern matching
- **math**: `math.floor`, `math.ceil`, `math.random`, `math.sqrt`, `math.abs`, `math.max`, `math.min`, `math.pi`
- **utf8**: `utf8.char`, `utf8.codepoint`, `utf8.len`

Más los built-ins de Lua que no requieren cargar librerías: `type()`, `tostring()`, `tonumber()`, `pairs()`, `ipairs()`, `next()`, `select()`, `pcall()`, `xpcall()`, `error()`, `setmetatable()`, `getmetatable()`, `rawget()`, `rawset()`, `rawequal()`, `rawlen()`. Los closures, variables locales, metatables, todo el control de flujo — todo lo que hace que Lua sea _Lua_ funciona sin problema.

### Qué está bloqueado

| Librería    | Por qué está bloqueada                                                                                                                                     |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `io`        | Lectura/escritura de archivos. No se puede permitir que los hooks lean archivos arbitrarios o escriban en disco.                                          |
| `os`        | Llamadas al sistema: `os.execute()` sería un escape completo a shell, `os.remove()` puede borrar archivos. Incluso `os.getenv()` se reemplaza con el `dbflux.env.get()` gated. |
| `debug`     | `debug.sethook()` podría interferir con el interrupt basado en conteo de instrucciones. `debug.getlocal()` y `debug.getinfo()` podrían inspeccionar estado interno. |
| `package`   | `require()`, `dofile()`, `loadfile()` permitirían cargar código arbitrario desde disco.                                                                    |
| `coroutine` | No es peligrosa en sí misma, pero agrega complejidad al modelo de timeout/cancelación (las coroutines pueden hacer yield más allá del instruction hook).   |

El sandbox es "allowlist, no blocklist". Solo existen las cuatro librerías cargadas explícitamente más las funciones de API registradas. Si no está en la lista de arriba, no existe en la VM de Lua.

### Límite de memoria

Cada VM se crea con un cap de memoria forzado de 16 MiB (`lua.set_memory_limit(16 * 1024 * 1024)` en `engine.rs`). Un script que asigna más allá de este límite falla con un error de memoria en lugar de que se le permita agotar la memoria del host.

---

## La API de Lua

### `hook.*` — Siempre disponible

Esta es la API central de control de flujo. Cada script de hook de Lua comunica su resultado a través de estas funciones.

```lua
-- Read the current phase
local phase = hook.phase  -- "pre_connect", "post_connect", ...

-- Signal outcomes
hook.ok()           -- success (this is the default if nothing is called)
hook.warn("msg")    -- success, but surface a warning to the user
hook.fail("msg")    -- failure, abort the connection flow
```

El outcome es una máquina de estados simple con tres estados: `Ok`, `Warn(msg)`, `Fail(msg)`. **Las llamadas múltiples se sobrescriben** — solo importa la última llamada antes de que el script termine. Si el script se completa sin llamar a ninguna de estas, el outcome por defecto es `Ok`.

El outcome se mapea a `HookResult` así:

| Outcome     | `exit_code` | `stderr` | `warnings` |
| ----------- | ----------- | -------- | ---------- |
| `Ok`        | `0`         | vacío    | `[]`       |
| `Warn(msg)` | `0`         | vacío    | `[msg]`    |
| `Fail(msg)` | `1`         | `msg`    | `[]`       |

### `connection.*` — Metadata de conexión

Gated por `capabilities.connection_metadata` (por defecto: **true**).

```lua
connection.profile_id     -- "550e8400-e29b-41d4-a716-446655440000"
connection.profile_name   -- "Production DB"
connection.db_kind        -- "Postgres", "SQLite", "MongoDB", "Redis", "MySQL"
connection.host           -- "db.example.com" or nil (SQLite has no host)
connection.port           -- 5432 or nil
connection.database       -- "myapp" or nil
```

Todos los valores son **snapshots estáticos** tomados en el momento de crear la VM. El script no puede cambiarlos. Esto es intencional — los hooks observan la conexión, no la configuran.

### `dbflux.log.*` — Logging

Gated por `capabilities.logging` (por defecto: **true**).

```lua
dbflux.log.info("Starting SSO flow")
dbflux.log.warn("Token expires in 5 minutes")
dbflux.log.error("AWS CLI not found")
```

Cada llamada hace dos cosas:

1. Agrega `[LEVEL] message` a un buffer de log interno (que se convierte en el `stdout` del `HookResult`)
2. Reenvía al crate `log` de Rust en el nivel correspondiente, con el prefijo `[lua]`

Cuando el caller provee un canal de salida, la misma línea de log también se transmite inmediatamente a la UI. El buffer de log sigue siendo la salida durable primaria para el `HookResult` final.

### `dbflux.env.*` — Variables de entorno

Gated por `capabilities.env_read` (por defecto: **true**).

```lua
local home = dbflux.env.get("HOME")          -- "/home/user" or nil
local profile = dbflux.env.get("AWS_PROFILE") -- "production" or nil

if not dbflux.env.get("DATABASE_URL") then
    hook.fail("DATABASE_URL is not set")
end
```

Solo lectura. Sin `set()` ni `unset()` — los hooks no pueden modificar el entorno. Esto reemplaza `os.getenv()`, que requeriría cargar la librería insegura `os`.

### `dbflux.process.*` — Ejecución de procesos controlada

Gated por `capabilities.process_run` (por defecto: **false**). Debe habilitarse explícitamente.

Incluso cuando está habilitada, la API de procesos tiene **doble gate** mediante un sistema de allowlist. No puedes ejecutar programas arbitrarios — solo herramientas específicas de categorías predefinidas.

```lua
local result = dbflux.process.run({
    program = "aws",
    allowlist = "aws_cli",
    args = { "sso", "login", "--profile", "prod" },
    timeout_ms = 120000,
    cwd = "/home/user",
    stream = true,
})

if not result.ok then
    hook.fail("AWS SSO login failed: " .. result.stderr)
end

dbflux.log.info("AWS SSO login succeeded")
hook.ok()
```

**Opciones de entrada:**

| Campo        | Tipo     | Requerido | Descripción                                                             |
| ------------ | -------- | --------- | ------------------------------------------------------------------------ |
| `program`    | string   | sí        | Nombre de comando plano (sin separadores de ruta)                        |
| `allowlist`  | string   | sí        | Debe coincidir con un nombre de allowlist conocido                       |
| `args`       | string[] | no        | Argumentos del comando                                                   |
| `timeout_ms` | integer  | no\*      | Timeout por proceso (ms). El timeout a nivel de hook igual aplica por encima. |
| `cwd`        | string   | no        | Directorio de trabajo                                                    |
| `stream`     | boolean  | no        | Transmite stdout/stderr al caller mientras el proceso sigue en ejecución |
| `detached`   | boolean  | no        | Entrega el proceso spawneado de vuelta al caller y retorna inmediatamente, en lugar de esperar a que termine |

\* Para un `run` no detached, `timeout_ms` es efectivamente requerido cuando no hay timeout a nivel de hook: una llamada sin `timeout_ms` ni timeout a nivel de hook falla con el error de runtime `"dbflux.process.run requires a timeout_ms when no hook-level timeout is set"`. Un `run` detached está exento de esta restricción.

**Valor de retorno:**

| Campo       | Tipo        | Descripción                                        |
| ----------- | ----------- | --------------------------------------------------- |
| `ok`        | boolean     | `true` si el proceso fue detached, o si el exit code es 0 y no hubo timeout |
| `detached`  | boolean     | `true` si el proceso se entregó como detached (en cuyo caso los campos de output/exit de abajo quedan vacíos/nil) |
| `exit_code` | integer/nil | Exit code del proceso                               |
| `stdout`    | string      | stdout capturado                                     |
| `stderr`    | string      | stderr capturado                                     |
| `timed_out` | boolean     | `true` si se disparó el timeout por proceso          |

**Allowlists disponibles:**

| Allowlist     | Programas permitidos                             |
| ------------- | ------------------------------------------------- |
| `aws_cli`     | `aws`, `aws.exe`                                   |
| `python_cli`  | `python`, `python.exe`, `python3`, `python3.exe`   |
| `ssh_cli`     | `ssh`, `ssh.exe`                                   |
| `cloudflared` | `cloudflared`, `cloudflared.exe`                   |
| `gcloud_cli`  | `gcloud`, `gcloud.cmd`, `gcloud.exe`               |
| `az_cli`      | `az`, `az.cmd`, `az.exe`                           |

`program` debe ser un **nombre de comando plano**. Los nombres calificados con ruta se rechazan antes de verificar la allowlist: cualquier programa que contenga un `/` o `\`, que esté compuesto de múltiples componentes de ruta, o que empiece con `~` falla con el error de runtime `"Program '...' must be a bare command name (no path separators)"`. Así que `program = "/usr/local/bin/aws"` se rechaza directamente — pasa `program = "aws"` en su lugar y deja que se resuelva vía `PATH`. La comparación del nombre plano contra la allowlist no distingue mayúsculas/minúsculas.

Este diseño responde a un caso de uso específico: hooks que necesitan disparar herramientas de CLI en la nube (login SSO, configuración de túnel, obtención de secrets) sin abrir un escape completo a shell. La allowlist es una guardia de ergonomía y footgun — previene typos y ejecución accidental de programas inesperados. **No** es un límite de aislamiento de seguridad: un usuario que controla `PATH` aún puede sustituir un binario diferente bajo el mismo nombre. Las allowlists hardcodeadas se pueden extender más adelante a medida que surjan nuevos casos de uso.

---

## Timeout y cancelación

Hay tres capas de interrupción, y entender cómo interactúan es importante.

### Capa 1: instruction hook de Lua

```rust
lua.set_hook(
    HookTriggers::new().every_nth_instruction(1_000),
    move |_lua, _debug| { ... }
);
```

Cada 1.000 instrucciones de Lua, el hook se dispara y verifica:

1. ¿Está seteado el cancel token? → `RuntimeError("Lua hook cancelled")`
2. ¿Se agotó el timeout? → `RuntimeError("Lua hook timed out")`

Esto captura loops infinitos, cómputos descontrolados y código Lua puro de larga duración. El intervalo de 1.000 instrucciones es un balance entre responsividad (verificar seguido) y performance (verificar no es gratis).

**Limitación**: este hook solo se dispara para instrucciones de bytecode de Lua. Si el script llama a una función bloqueante de Rust (como `dbflux.process.run`), el instruction hook no se dispara hasta que esa función retorna. Por eso...

### Capa 2: Process Executor compartido

Dentro de `dbflux.process.run`, la ejecución de procesos se delega al helper compartido `dbflux_core::execute_streaming_process()`. Ese helper:

- crea threads lectores para stdout y stderr
- empuja chunks de output a través de un canal
- verifica cancel tokens y timeouts en un intervalo corto
- mata al proceso hijo en caso de cancelación o timeout
- retorna una tabla de resultado normal para el timeout por proceso, o un error de runtime de Lua para cancelación/timeout a nivel de hook

Esto mantiene alineados los hooks de Lua y los hooks de script que no son Lua. Se usa el mismo camino de ejecución de procesos de bajo nivel para subprocesos disparados desde Bash, Python y Lua.

### Capa 3: Parent cancel token

El flujo de conexión pasa un parent cancel token que cancela todos los hooks cuando se aborta la operación general de connect/disconnect. Tanto el instruction hook como el process executor compartido verifican este token junto con el específico del hook.

### Jerarquía de timeouts

```
Hook-level timeout (e.g., 30s)
  └── Process-level timeout (e.g., 120s for SSO login)
        └── Actually, process timeout < hook timeout to be useful
```

Si el timeout a nivel de hook se dispara mientras un proceso está corriendo, el proceso se mata y todo el hook aborta con un error de timeout de Lua, que `LuaExecutor` convierte en `HookResult { timed_out: true }`.

Si el timeout a nivel de proceso se dispara, solo ese proceso se mata. El script sigue ejecutándose y puede manejar el timeout con gracia:

```lua
local result = dbflux.process.run({ ..., timeout_ms = 5000 })
if result.timed_out then
    dbflux.log.warn("Process timed out, falling back to cached credentials")
end
```

---

## Manejo de errores

### Cómo fluyen los errores

```
Script execution
    │
    ├─ Completes normally → outcome (Ok/Warn/Fail) determines HookResult
    │
    ├─ "Lua hook cancelled" → Err(String) returned to caller
    │                          (the ONLY case that returns Err)
    │
    ├─ "Lua hook timed out" → Ok(HookResult { timed_out: true })
    │
    └─ Any other Lua error → Ok(HookResult { exit_code: 1, stderr: error_msg })
```

La cancelación es el único caso que retorna `Err` desde `execute_hook`. Los timeouts y errores de runtime son outcomes normales de "el hook falló" y se capturan en `HookResult`.

### Detección de errores basada en sentinelas

mlua envuelve los errores en capas de `CallbackError` y `WithContext`. Para detectar cancelación vs. timeout, el código usa una función recursiva `error_has_message` que desenvuelve estas capas buscando las cadenas sentinela exactas `"Lua hook cancelled"` y `"Lua hook timed out"`.

Esta es una solución pragmática. Un approach más limpio sería usar tipos de error personalizados, pero el modelo de errores de mlua hace eso poco práctico sin luchar contra la librería. El approach de sentinelas funciona de forma confiable porque estas cadenas exactas solo son producidas por nuestro instruction hook y el camino de ejecución de procesos compartido.

---

## LuaCapabilities

Definido en `dbflux_core::connection::hook`:

```rust
pub struct LuaCapabilities {
    pub logging: bool,              // default: true
    pub env_read: bool,             // default: true
    pub connection_metadata: bool,  // default: true
    pub process_run: bool,          // default: false
}
```

Estos se configuran por hook en la UI de Settings. Los defaults son deliberadamente conservadores — `process_run` es la única capability peligrosa, y está deshabilitada por defecto.

Las verificaciones de capability ocurren en el momento de crear la VM, no en el momento de la llamada. Si `logging` es false, la tabla `dbflux.log` simplemente no existe en la VM. No hay verificación en runtime; el sandbox es estructural.

---

## Detalles internos de arquitectura

### LuaRuntimeState

```rust
pub struct LuaRuntimeState {
    pub outcome: Arc<Mutex<LuaHookOutcome>>,
    pub log_buffer: Arc<Mutex<Vec<String>>>,
    pub output: Option<OutputSender>,
    pub detached: Option<DetachedProcessSender>,
    pub cancel_token: CancelToken,
    pub parent_cancel_token: Option<CancelToken>,
    pub hook_started_at: Instant,
    pub hook_timeout: Option<Duration>,
}
```

Este es el estado mutable compartido al que acceden tanto los callbacks de Lua como el executor. El patrón `Arc<Mutex<...>>` es necesario porque los closures de Lua (registrados como funciones de API) capturan `Arc`s clonados, y el executor lee el estado final después de la ejecución del script.

El sender de `output` es opcional. Cuando está presente, las llamadas de log de Lua y `dbflux.process.run({ stream = true })` reenvían output en vivo a la UI mientras siguen preservando el output buffereado final en `HookResult`.

El `cancel_token` y los campos de timing también se comparten con la ejecución de procesos, creando una única vista del contexto de ejecución a través de todas las capas.

### LuaVmConfig

`LuaEngine::create_vm()` toma un struct `LuaVmConfig` en lugar de una lista larga de argumentos. Agrupa el contexto del hook, la fase, las capabilities, el estado de cancelación, el sender de output opcional y la metadata de timeout necesaria para construir una VM nueva.

### LuaVm

```rust
pub struct LuaVm {
    pub lua: Lua,
    pub state: LuaRuntimeState,
}
```

Agrupa la VM de Lua y el estado compartido para que el executor pueda acceder a ambos. Después de que `vm.lua.load(&script).exec()` se completa, el executor lee `vm.state.log_buffer` y `vm.state.outcome` para construir el `HookResult`.

### El patrón de lazy init de la tabla `dbflux`

```rust
fn ensure_dbflux_table(lua: &Lua) -> LuaResult<Table> {
    let globals = lua.globals();
    match globals.get::<Table>("dbflux") {
        Ok(table) => Ok(table),
        Err(_) => {
            let table = lua.create_table()?;
            globals.set("dbflux", table.clone())?;
            Ok(table)
        }
    }
}
```

Cada función `register_*_api` llama a esto para obtener-o-crear el global `dbflux`. Esto permite que las capabilities se registren de forma independiente sin conocerse entre sí — cada una simplemente agrega su sub-tabla al padre compartido.

---

## Guía de estilo de scripts

Basándose en los casos de test y el diseño de la API, así es la forma idiomática de escribir hooks de Lua:

### Hook básico

```lua
dbflux.log.info("Pre-connect hook for " .. connection.profile_name)

if connection.db_kind == "Postgres" and hook.phase == "pre_connect" then
    local db_url = dbflux.env.get("DATABASE_URL")
    if not db_url then
        hook.fail("DATABASE_URL environment variable is not set")
        return
    end
end

hook.ok()
```

### Hook de login SSO

```lua
local result = dbflux.process.run({
    program = "aws",
    allowlist = "aws_cli",
    args = { "sso", "login", "--profile", connection.profile_name },
    timeout_ms = 120000,
})

if not result.ok then
    hook.fail("AWS SSO login failed: " .. result.stderr)
    return
end

dbflux.log.info("AWS SSO login completed successfully")
hook.ok()
```

### Condicional por fase

```lua
if hook.phase == "pre_connect" then
    dbflux.log.info("Establishing tunnel...")
    -- setup logic
elseif hook.phase == "post_disconnect" then
    dbflux.log.info("Cleaning up...")
    -- teardown logic
end
```

### Patrón de manejo de errores

```lua
-- Use pcall for operations that might fail
local ok, err = pcall(function()
    -- risky operations here
end)

if not ok then
    hook.fail("Unexpected error: " .. tostring(err))
    return
end
```

### Convenciones

- **Usa `return` después de `hook.fail()`** — el script sigue ejecutándose después de `hook.fail()`, que simplemente setea un flag. Si no haces return, código posterior podría llamar a `hook.ok()` y sobrescribir el fallo. Gana la última llamada.
- **Loguea con generosidad** — el output de `dbflux.log.info()` aparece en el result panel. Es la única forma de comunicar progreso y depurar problemas.
- **Verifica `result.ok`, no `result.exit_code`** — el campo `ok` considera tanto el exit code como el timeout. `exit_code` puede ser `nil` en casos límite.
- **No dependas de que `hook.phase` esté ausente en el editor** — cuando se ejecuta un script desde el botón Run del editor de código (no como parte de un flujo de conexión), la fase por defecto es `"pre_connect"`. La lógica dependiente de fase debe manejar esto con gracia.

---

## Limitaciones

### Sin async

Todo es síncrono y bloqueante. La VM de Lua corre en un thread en background, y `dbflux.process.run` bloquea ese thread hasta que el process executor compartido termina. Para la mayoría de los casos de uso de hooks (llamadas a herramientas CLI, verificaciones de entorno), esto está bien. Pero no puedes hacer requests HTTP asíncronos ni operaciones en paralelo.

### Sin acceso a red

No hay cliente HTTP, librería de sockets, ni API de red. La única forma de interactuar con servicios externos es a través de `dbflux.process.run` con una herramienta CLI en la allowlist. Esto es intencional — un cliente HTTP sandboxed necesitaría filtrado cuidadoso de URLs y expandiría significativamente la superficie de ataque.

### Sin I/O de archivos

Sin `io.open`, sin `os.rename`, sin lectura o escritura directa de archivos desde Lua mismo. Si necesitas datos del mundo exterior, tienes que pasar por un proceso permitido en la allowlist como Python o un CLI de nube.

### Sin estado persistente

Cada ejecución de hook crea una VM nueva. No hay forma de guardar estado entre invocaciones. Si necesitas estado persistente, escríbelo a un archivo a través de un proceso externo y léelo de vuelta en la siguiente invocación.

### Sin `require()`

La librería `package` no se carga, así que `require()` no existe. No puedes dividir código Lua en múltiples archivos ni usar librerías de Lua de terceros. Toda la lógica del hook debe ser autocontenida en un único script.

### Sin `os.time()` ni `os.clock()`

La librería `os` está bloqueada por completo. Si necesitas timing, tendrás que medirlo externamente. Esto también significa que `math.randomseed(os.time())` no funciona — `math.random()` usa el seed que sea que mlua provea (que depende de la implementación).

### Allowlists limitadas

Las allowlists de procesos están hardcodeadas. Agregar una herramienta nueva requiere un cambio de código, un rebuild y un release nuevo. No hay (por ahora) un mecanismo de allowlist configurable por el usuario. Las seis allowlists actuales cubren los casos de uso más comunes (CLIs de nube, SSH, scripts de Python).

### Sin syntax highlighting de Lua en el editor

gpui-component (v0.5.0) no incluye una grammar `tree-sitter-lua`. Al editar scripts de Lua en el editor de código, no hay syntax highlighting. `editor_mode()` retorna `"lua"`, que cae con gracia a plaintext. Los scripts de Python y Bash tienen highlighting completo.

### Memoria acotada

Cada VM tiene un cap de 16 MiB de memoria asignada por Lua. Los scripts que intentan construir estructuras de datos muy grandes en memoria van a golpear este techo y fallar. Esta es una guardia de sandbox, no un ajuste configurable por hook.

### El output está impulsado por API

La forma soportada de comunicar progreso y diagnósticos es `dbflux.log.*`. Ese output se buffea en el `HookResult` final, y también puede transmitirse en vivo cuando el caller lo solicita.

---

## Cómo se integra en la aplicación

### Feature flag

La dependencia opcional `dbflux_lua` y su feature `lua` viven en el app crate, `crates/dbflux_app/Cargo.toml`:

```toml
dbflux_lua = { workspace = true, optional = true }
# ...
[features]
lua = ["dbflux_lua"]
```

El binary crate, `crates/dbflux/Cargo.toml`, no tiene dependencia directa de `dbflux_lua`. Su feature `lua` simplemente reenvía a los crates de app y UI, y forma parte del set por defecto:

```toml
[features]
lua = ["dbflux_app/lua", "dbflux_ui/lua"]
default = ["sqlite", "postgres", "mysql", "mongodb", "redis", "dynamodb", "cloudwatch", "influxdb", "mssql", "lua", "aws", "mcp"]
```

El feature `lua` está en el set por defecto, así que siempre está habilitado en builds normales. Se puede deshabilitar para builds que no necesitan Lua (reduce el tamaño del binario en ~200KB).

### CompositeExecutor

`crates/dbflux_app/src/hook_executor.rs` define el router (re-exportado desde `crates/dbflux_app/src/lib.rs`):

```rust
#[derive(Clone)]
pub struct CompositeExecutor {
    process: ProcessExecutor,
    #[cfg(feature = "lua")]
    lua: dbflux_lua::LuaExecutor,
}
```

`HookKind::Lua` se enruta a `LuaExecutor`. `HookKind::Command` y `HookKind::Script` van a `ProcessExecutor`. Sin el feature `lua`, los hooks de Lua retornan un mensaje de error.

### Integración con el botón Run

El botón Run del editor de código (`execution.rs`) usa `CompositeExecutor` para ejecutar scripts. Para scripts de Lua, crea un `ConnectionHook` inline a partir del contenido del editor con `LuaCapabilities::all_enabled()` y un timeout de 30 segundos, pasa un canal de output a `execute_hook`, y renderiza output en vivo en el results panel mientras el script sigue en ejecución. El stdout final (buffer de log) y el stderr se siguen preservando en el resultado de texto completado.

---

## Testing

Todos los tests están en el crate mismo (no en un directorio `tests/` separado). La cobertura actualmente abarca:

- `executor.rs`: outcomes normales, errores de runtime, scripts respaldados por archivo, cancelación, timeouts, gating de capabilities, enforcement de allowlist, y comportamiento de output de procesos transmitidos
- `engine.rs`: fase del hook, metadata de conexión, librerías inseguras ocultas, visibilidad opcional de API, y comportamiento de construcción de VM
- `api/dbflux.rs`: validación de opciones de proceso, manejo de timeout de hook expirado antes de spawnear, formateo de eventos de log en vivo, y stdout/stderr parcial transmitido durante cancelación

### Ejecutar los tests

```bash
cargo test -p dbflux_lua           # all tests
cargo test -p dbflux_lua -- timeout  # specific test by name
```

Algunos tests spawnean procesos reales (`echo`, `sleep`, `python3`) y tienen timeouts, así que toman uno o dos segundos. Los tests relacionados con procesos usan `cfg!(target_os = "windows")` para seleccionar comandos apropiados por plataforma.

---

## Lecciones y trampas

### El problema del envoltorio de errores de mlua

mlua envuelve los errores en múltiples capas: `CallbackError { cause: WithContext { context: "...", cause: RuntimeError("actual message") } }`. Cuando quieres detectar un error específico (como "Lua hook cancelled"), no puedes simplemente hacer match sobre la variante externa — tienes que desenvolver recursivamente. La función `error_has_message` hace esto, pero es frágil. Si mlua cambia su comportamiento de envoltorio, la detección de sentinelas se rompe en silencio.

Un approach mejor podría ser usar `Error::external()` de mlua con un tipo de error personalizado que implemente `std::error::Error`, pero el approach de sentinelas actual se ha mantenido bien a través de las versiones de mlua.

### El intervalo de 1.000 instrucciones

El instruction hook se dispara cada 1.000 instrucciones. Esto significa:

- Un loop ajustado que no hace nada toma ~1.000 iteraciones antes de que se dispare la verificación de cancelación
- Para precisión de timeout, 1.000 instrucciones se traducen a aproximadamente microsegundos, así que la precisión del timeout es excelente
- Setearlo demasiado bajo (p. ej., cada instrucción) impacta medible en la performance de scripts computacionales
- Setearlo demasiado alto (p. ej., cada 100.000) hace que la cancelación se sienta lenta

1.000 balancea la responsividad de cancelación contra el overhead por verificación.

### La estratificación de timeout en process_run

El timeout de tres capas (instruction hook, process executor compartido, timeout por proceso) puede resultar confuso. La observación clave: **el timeout a nivel de proceso es recuperable** (el script continúa), **el timeout a nivel de hook no lo es** (el hook falla). Así que siempre debes setear `timeout_ms` en las llamadas a `dbflux.process.run` a algo menor que el timeout del hook, permitiendo que el script maneje el fallo con gracia.

### ¿Por qué no simplemente permitir `os.execute()`?

Podría parecer más simple cargar la librería `os` y dejar que los usuarios corran lo que quieran. El problema es que `os.execute()` no provee captura de output, sin timeout, sin cancelación, y sin filtrado de programas. La API `dbflux.process.run` nos da todo esto. La allowlist es el precio de restringir qué programas puede lanzar un hook en una app GUI que ejecuta scripts de usuario. Es una guardia de ergonomía/footgun más que un límite de aislamiento de seguridad — la sustitución vía PATH todavía puede intercambiar el binario detrás de un nombre permitido.

### VM nueva por ejecución — costo vs. seguridad

Crear una VM de Lua 5.4 nueva por invocación de hook cuesta ~0.5ms. Para algo que corre como máximo 4 veces por ciclo de vida de conexión, esto es despreciable. El beneficio — aislamiento perfecto entre ejecuciones — vale mucho más que el costo. Un approach de VM pooled ahorraría microsegundos pero introduciría bugs sutiles de filtrado de estado.
