# Sistema de Audit de DBFlux

DBFlux registra todas las operaciones significativas en un audit trail unificado
almacenado en SQLite. Esto cubre la ejecución de queries, el ciclo de vida de
las conexiones, la ejecución de hooks, las ejecuciones de scripts, las
decisiones de governance de MCP y los cambios de configuración.

## Ubicación de almacenamiento

Todos los audit events se almacenan en la base de datos unificada:

```
~/.local/share/dbflux/dbflux.db
```

Tabla: `aud_audit_events`

La misma base de datos almacena todo el resto del estado en tiempo de ejecución
(profiles, history, sessions). El schema lo gestiona el sistema de migraciones
en `dbflux_storage/src/migrations/`.

## Estructura de un evento

Cada audit event es un `EventRecord` (`dbflux_core/src/observability/types.rs`)
con estos campos:

| Campo            | Tipo             | Descripción                                                     |
| ---------------- | ---------------- | --------------------------------------------------------------- |
| `id`             | `i64`            | Asignado automáticamente al insertar                            |
| `ts_ms`          | `i64`            | Timestamp Unix en milisegundos                                  |
| `level`          | `EventSeverity`  | `trace`, `debug`, `info`, `warn`, `error`, `fatal`              |
| `category`       | `EventCategory`  | Dominio del evento (ver abajo)                                  |
| `action`         | `String`         | Identificador específico de la acción (p. ej., `query_execute`) |
| `outcome`        | `EventOutcome`   | `success`, `failure`, `cancelled`, `pending`                    |
| `actor_type`     | `EventActorType` | Quién disparó el evento                                         |
| `actor_id`       | `Option<String>` | Identidad del actor (client ID de MCP, nombre del hook, etc.)   |
| `source_id`      | `EventSourceId`  | Dónde se originó el evento                                      |
| `connection_id`  | `Option<String>` | ID del connection profile                                       |
| `database_name`  | `Option<String>` | Nombre de la base de datos destino                              |
| `driver_id`      | `Option<String>` | ID del driver (p. ej., `postgres`, `mongodb`)                   |
| `object_type`    | `Option<String>` | Tipo de objeto afectado (p. ej., `table`, `collection`)         |
| `object_id`      | `Option<String>` | ID/nombre del objeto específico                                 |
| `summary`        | `String`         | Descripción legible por humanos                                 |
| `details_json`   | `Option<String>` | Contexto estructurado adicional como objeto JSON                |
| `error_code`     | `Option<String>` | Código de error en caso de fallo                                |
| `error_message`  | `Option<String>` | Mensaje de error en caso de fallo                               |
| `duration_ms`    | `Option<i64>`    | Tiempo de ejecución en milisegundos                             |
| `session_id`     | `Option<String>` | ID de correlación de sesión                                     |
| `correlation_id` | `Option<String>` | ID de correlación entre componentes                             |

### Categorías de eventos

| Categoría       | String           | Qué captura                                                                                                        |
| --------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------ |
| `Query`         | `query`          | Ejecución de SQL, queries de MongoDB, operaciones de scan                                                          |
| `Connection`    | `connection`     | Ciclo de vida de connect, disconnect, reconnect                                                                    |
| `Hook`          | `hook`           | PreConnect, PostConnect, PreDisconnect, PostDisconnect                                                             |
| `Script`        | `script`         | Ejecución de scripts Lua, Python, Bash                                                                             |
| `Mcp`           | `mcp`            | Llamadas a tools de un AI client y decisiones de policy                                                            |
| `Governance`    | `governance`     | Resultados de evaluación de policy                                                                                 |
| `Config`        | `config`         | Cambios de profile, modificaciones de settings                                                                     |
| `System`        | `system`         | Arranque de la aplicación, panics, migraciones                                                                     |
| `ObjectStorage` | `object_storage` | Eventos de CRUD/mutación de object storage (upload, delete, presign, rename, create bucket/folder, save-back edit) |

### Tipos de actor

| Tipo        | String       | Significado                           |
| ----------- | ------------ | ------------------------------------- |
| `User`      | `user`       | Humano operando la GUI de DBFlux      |
| `System`    | `system`     | Operación de sistema en segundo plano |
| `App`       | `app`        | Aplicación actuando de forma autónoma |
| `McpClient` | `mcp_client` | AI agent vía el protocolo MCP         |
| `Hook`      | `hook`       | Script de un lifecycle hook           |
| `Script`    | `script`     | Script escrito por el usuario         |

### Campos obligatorios por categoría

La validación la aplica `AuditService::validate_event()` antes de almacenar:

| Categoría              | Obligatorio además de `action` + `summary`                              |
| ---------------------- | ----------------------------------------------------------------------- |
| `Query`                | `connection_id`, `driver_id`, `duration_ms` (para eventos de ejecución) |
| `Connection`           | `connection_id`                                                         |
| `Hook`                 | `object_type`, `object_id`, `connection_id`                             |
| `Script`               | `object_type`, `object_id`                                              |
| `Mcp`                  | `actor_id`, `object_id` (nombre de la tool)                             |
| `Config`               | `object_type`, `object_id`                                              |
| `ObjectStorage`        | `connection_id`, `object_type`, `object_id`                             |
| `Governance`, `System` | Sin campos adicionales                                                  |

## Privacidad y redacción

Por defecto, `AuditService` se ejecuta con estos ajustes:

- **`redact_sensitive = true`**: los valores sensibles (contraseñas, tokens,
  connection strings) en `details_json` y `error_message` se reemplazan por
  `[REDACTED]` antes de almacenarse.
- **`capture_query_text = false`**: el texto completo de la query nunca se
  almacena. En su lugar, se guardan un fingerprint SHA256 más la longitud
  original como `[FINGERPRINT:<16-char-hex>]` con `query_length`. Esto evita que
  datos sensibles en las queries se filtren al audit log.
- **`max_detail_bytes = 65536`**: los payloads mayores de 64 KiB se rechazan
  para prevenir el crecimiento descontrolado del almacenamiento.

Estos ajustes se pueden cambiar en tiempo de ejecución vía los métodos
`AuditService::set_*()`. El servidor MCP expone algunos de ellos a través de la
configuración de governance.

## Ver los audit events

### En la UI de DBFlux

Navega a **Workspace → Audit**. La vista unificada de audit soporta:

- Filtrado por actor, tool/action, rango de fechas, decision, category
- Exportar los resultados filtrados a CSV o JSON

El mismo shell de UI `AuditDocument` se reutiliza también para los event streams
externos respaldados por un driver, cuando un driver los declara a través de
abstracciones genéricas del core (`CollectionPresentation`,
`CollectionChildInfo`, `EventStreamTarget`). La UI no debe hacer casos
especiales para drivers concretos al abrir o renderizar esos streams.

### Directamente vía SQLite

La base de datos es un archivo SQLite estándar. Consúltala directamente:

```bash
sqlite3 ~/.local/share/dbflux/dbflux.db
```

Queries útiles:

```sql
-- All events in the last 24 hours
SELECT id, datetime(ts_ms/1000, 'unixepoch') as ts, level, category, action, outcome, actor_id, summary
FROM aud_audit_events
WHERE ts_ms &gt; (unixepoch('now') - 86400) * 1000
ORDER BY ts_ms DESC;

-- MCP tool calls only
SELECT id, datetime(ts_ms/1000, 'unixepoch') as ts, actor_id, object_id as tool, outcome, summary
FROM aud_audit_events
WHERE category = 'mcp'
ORDER BY ts_ms DESC;

-- All failed operations
SELECT id, datetime(ts_ms/1000, 'unixepoch') as ts, category, action, actor_id, error_message
FROM aud_audit_events
WHERE outcome = 'failure'
ORDER BY ts_ms DESC
LIMIT 50;

-- Query events by connection
SELECT id, datetime(ts_ms/1000, 'unixepoch') as ts, action, driver_id, duration_ms, summary
FROM aud_audit_events
WHERE category = 'query' AND connection_id = 'your-connection-id'
ORDER BY ts_ms DESC;

-- Events grouped by category and outcome
SELECT category, outcome, count(*) as count
FROM aud_audit_events
GROUP BY category, outcome
ORDER BY category, outcome;
```

### Vía MCP Tools (AI clients)

La superficie de tools de MCP expone tres audit tools (clasificadas dentro de la
execution class `read`):

```
query_audit_logs    — Filter events by actor, tool, date range, decision
get_audit_entry     — Retrieve a single event by ID
export_audit_logs   — Export filtered results as CSV or JSON
```

### Vía la API de Rust

```rust
use dbflux_audit::{AuditService, AuditQueryFilter, AuditExportFormat};

let service = AuditService::new_sqlite_default()?;

// Query recent events
let filter = AuditQueryFilter {
    category: Some("mcp".to_string()),
    start_epoch_ms: Some(start_ms),
    limit: Some(100),
    ..Default::default()
};
let events = service.query(&amp;filter)?;

// Export to CSV
let csv = service.export(&amp;filter, AuditExportFormat::Csv)?;

// Export extended (all fields including details_json)
let json = service.export_extended(&amp;filter, AuditExportFormat::Json)?;
```

## Generar audit events

### Desde las capas de servicio

Usa el trait `EventSink`. Todos los componentes que emiten audit events aceptan
un `Arc<dyn EventSink>`:

```rust
use dbflux_core::observability::{
    EventOrigin, EventRecord, EventSink,
    types::{EventCategory, EventSeverity, EventOutcome},
    actions,
};

// Build the event
let event = EventRecord::new(
    now_epoch_ms(),
    EventSeverity::Info,
    EventCategory::Query,
    EventOutcome::Success,
)
.with_typed_action(actions::QUERY_EXECUTE)
.with_summary("SELECT executed on users table")
.with_actor_id("my-actor-id")
.with_origin(EventOrigin::local())
.with_connection_context("my-profile-id", "mydb", "postgres")
.with_object_ref("table", "users")
.with_duration_ms(42);

// Emit through the sink (injected via constructor or DI)
event_sink.record(event)?;
```

### Constantes canónicas de acción

Las cadenas de action se definen en `dbflux_core/src/observability/actions.rs`.
Usa las constantes en lugar de strings sueltos:

| Constante                 | String                    | Categoría  |
| ------------------------- | ------------------------- | ---------- |
| `QUERY_EXECUTE`           | `query_execute`           | Query      |
| `QUERY_EXECUTE_FAILED`    | `query_execute_failed`    | Query      |
| `CONNECTION_CONNECT`      | `connection_connect`      | Connection |
| `CONNECTION_DISCONNECT`   | `connection_disconnect`   | Connection |
| `HOOK_EXECUTE`            | `hook_execute`            | Hook       |
| `HOOK_EXECUTE_FAILED`     | `hook_execute_failed`     | Hook       |
| `SCRIPT_EXECUTE`          | `script_execute`          | Script     |
| `SCRIPT_EXECUTE_FAILED`   | `script_execute_failed`   | Script     |
| `MCP_AUTHORIZE`           | `mcp_authorize`           | Mcp        |
| `MCP_APPROVE_EXECUTION`   | `mcp_approve_execution`   | Mcp        |
| `MCP_REJECT_EXECUTION`    | `mcp_reject_execution`    | Mcp        |
| `MCP_TOOL_EXECUTE`        | `mcp_tool_execute`        | Mcp        |
| `MCP_TOOL_EXECUTE_FAILED` | `mcp_tool_execute_failed` | Mcp        |
| `SYSTEM_PANIC`            | `system_panic`            | System     |

### Checklist de campos obligatorios

Antes de llamar a `record()`, asegúrate de que:

1. `action` está definido y no está vacío (usa una constante de `actions`)
2. `summary` está definido y no está vacío (legible por humanos, una frase)
3. Los campos específicos de la categoría están presentes (ver la tabla de
   arriba)
4. `details_json` es un objeto JSON válido si se proporciona — no un array ni un
   primitivo
5. `details_json` ocupa menos de 64 KiB

### Eventos de fallo

Para los fallos, establece outcome a `EventOutcome::Failure` y rellena
`error_code` y `error_message`:

```rust
let event = EventRecord::new(ts_ms, EventSeverity::Error, EventCategory::Query, EventOutcome::Failure)
    .with_typed_action(actions::QUERY_EXECUTE_FAILED)
    .with_summary("Query failed: syntax error")
    .with_connection("profile-id", Some("mydb"), Some("postgres"))
    .with_error("42601", "syntax error at or near \"SELEC\"");
```

`error_message` se redacta si contiene patrones sensibles. Usa `error_code` para
identificadores de error estables y legibles por máquina.

## Retención y purga

Los eventos se pueden purgar según una política de retención:

```rust
// Delete events older than 90 days, in batches of 500
let stats = service.purge_old_events(90, 500)?;
println!("Deleted {} events in {} batches", stats.deleted_count, stats.batches);
```

La purga se ejecuta en lotes para evitar transacciones de escritura largas. No
se ejecuta automáticamente — añádela a una tarea programada en segundo plano o a
un runbook de operaciones.

## Puente de Tracing a Audit

El puente de tracing captura los eventos estructurados emitidos por las macros
`log::*!` y `tracing::*!` en todos los crates de DBFlux y los escribe en la
misma tabla `aud_audit_events` sin requerir migración de los call sites.

### Flujo del evento

```mermaid
flowchart TD
    LOG["log::warn!(...)"] --&gt; BRIDGE["LogTracer (tracing-log)"]
    BRIDGE --&gt; EVENT["tracing event"]
    TRACING["tracing::info!(...)"] --&gt; EVENT
    EVENT --&gt; LAYER["AuditLayer::on_event"]
    LAYER --&gt;|level gate + recursion guard| CHANNEL["bounded mpsc::sync_channel (512)"]
    CHANNEL --&gt; DRAIN["drain thread"]
    DRAIN --&gt;|AuditService::record| TABLE[("aud_audit_events (SQLite)")]
```

### Categoría permitida por el puente

Todos los eventos capturados a través del puente se asignan a la categoría
`System`. Esta es la resolución de la V1: los eventos de log de formato libre no
llevan los campos estructurados (`connection_id`, `object_type`, `object_id`)
que otras categorías requieren, así que enrutarlos a `Connection` o `Config`
haría que `validate_event` los rechazara. El `PREFIX_CATEGORY_MAP` en
`dbflux_core/src/observability/tracing_bridge/category.rs` mapea prefijos de
módulo a categorías previstas con fines documentales, pero todas las categorías
resueltas se coaccionan a `System` en tiempo de ejecución.

### Umbral de captura

Solo se escriben en el audit store los eventos con nivel igual o superior al
`log_capture_min_level` configurado. `TRACE` y `DEBUG` se filtran de forma
estricta — nunca se escriben, sin importar el umbral configurado.

El umbral se almacena como un ordinal `u8` en un `Arc<AtomicU8>` y se actualiza
sin reinicializar el subscriber. El mapeo es:

| Severidad | Ordinal |
| --------- | ------- |
| Trace     | 0       |
| Debug     | 1       |
| Info      | 2       |
| Warn      | 3       |
| Error     | 4       |

El umbral por defecto es `Info` (ordinal 2).

### Configurar el umbral

En la UI de DBFlux: desplegable **Settings → Audit → Log Capture → Minimum
Level**. Al seleccionar un nivel y pulsar Save se persiste en
`cfg_audit_settings.log_capture_min_level` (columna añadida por la migración
014) y se aplica al puente de forma atómica — no requiere reiniciar.

Directamente en SQLite:

```sql
UPDATE cfg_audit_settings SET log_capture_min_level = 'warn';
```

Valores válidos: `trace`, `debug`, `info`, `warn`, `error`.

### Contador de descartes

Cuando el canal acotado está lleno (512 eventos por defecto, configurable vía
`BridgeConfig::queue_capacity`), el puente descarta el evento entrante en lugar
de bloquear, e incrementa un contador de descartes `Arc<AtomicU64>`. Esto evita
que la ruta de audit introduzca contrapresión en el código de la aplicación. El
contador de descartes actual es accesible vía `BridgeHandle::drop_count()` y se
expone a través de `AuditService::dropped_log_event_count()` para
observabilidad, pero no se persiste ni se muestra en la UI en la V1.

### Ventana de arranque

Existe un breve intervalo entre el inicio del proceso y la instalación del sink
durante el cual los eventos se capturan en el canal de drenaje pero aún no se
vuelcan a SQLite — el sink se instala después de que se construye `AppState` y
se completa la primera lectura de audit settings. Los eventos en tránsito
durante esta ventana se retienen en el canal acotado y se entregan una vez
instalado el sink. Si el canal se llena durante la ventana de arranque, los
eventos se descartan y se contabilizan.

### Guardia de recursión

Los eventos emitidos desde `dbflux_core::observability::tracing_bridge` quedan
excluidos del puente para evitar bucles de realimentación en los que los
diagnósticos del propio puente se retroalimenten a sí mismos. Esto se aplica
mediante la constante `BRIDGE_INTERNAL_TARGET`, comprobada en
`AuditLayer::on_event`.

### Lista de permitidos por target

Solo los eventos cuyo `target` empiece por `dbflux` se reflejan en el audit
store. Dependencias upstream como `gpui`, `blade_graphics`, `naga`, `wgpu`,
`hyper` y `tokio` emiten trazas verbosas de nivel `INFO` (ciclo de vida de
texturas y buffers del render-loop, modo de presentación de superficie, ciclo de
vida de peticiones HTTP, etc.) que de otro modo inundarían el audit log de ruido
operacional sin ningún valor para el diagnóstico posterior.

Estos eventos siguen fluyendo a través de la capa fmt y permanecen visibles en
stderr (o en el archivo de log) según `RUST_LOG`. El gate vive en
`passes_target_gate`, en `layer.rs`, y se ejecuta antes de construir el record.

Para auditar un evento de una fuente que no sea `dbflux`, envuelve la emisión en
un módulo de dbflux y reemítela con un target de dbflux — el puente
intencionalmente no deja pasar targets upstream.

### Campos de tracing con nombre

El puente reconoce estos campos con nombre en los eventos de tracing y los mapea
a campos de `EventRecord`:

| Campo de tracing | Campo de `EventRecord`              |
| ---------------- | ----------------------------------- |
| `message`        | `summary`                           |
| `category`       | `category` (coaccionado a `System`) |
| `actor_type`     | `actor_type`                        |
| `actor_id`       | `actor_id`                          |
| `connection_id`  | `connection_id`                     |
| `database_name`  | `database_name`                     |
| `driver_id`      | `driver_id`                         |
| `action`         | `action`                            |
| `outcome`        | `outcome`                           |
| `details_json`   | `details_json`                      |

Los campos desconocidos se acumulan en `details_json` como un objeto JSON. Si el
mensaje excede los 512 caracteres, se trunca con `…` y el mensaje completo se
guarda en `details_json["message"]`.

El puente también mapea `correlation_id` directamente a
`EventRecord.correlation_id` (no dentro de `details_json`), lo que permite la
correlación entre componentes entre los toasts de error orientados al usuario y
sus audit records correspondientes.

### Eventos de error orientados al usuario

Los errores orientados al usuario (fallos de storage, errores de driver,
problemas de red, fallos al persistir la configuración) se reportan a través de
`report_error` / `report_error_async` desde `dbflux_ui_base::user_error`. Cada
llamada emite un evento de tracing que fluye a través del puente y además
dispara una notificación toast.

La forma del evento de tracing:

| Campo de tracing | Valor                                                                                      |
| ---------------- | ------------------------------------------------------------------------------------------ |
| `target`         | `dbflux_ui::user_error`                                                                    |
| `action`         | `user_error`                                                                               |
| `outcome`        | `failure`                                                                                  |
| `kind`           | `ErrorKind` como string (`storage`, `network`, `auth`, `hook`, `driver`, `user`, `config`) |
| `correlation_id` | UUID v7 que vincula el toast con el audit record                                           |
| `message`        | El resumen legible por humanos mostrado en el toast                                        |

El campo `correlation_id` lo extrae `AuditFieldVisitor` hacia
`EventRecord.correlation_id`. Nótese que el visitor enruta tanto `record_str`
(sigilo Display `%val`) como `record_debug` (sigilo Debug `?val`) a través del
mismo dispatcher `record_string_by_name`, de forma que los nuevos slots tipados
que se añadan en el futuro se recogen sin importar qué sigilo use quien llama.

Hay dos caminos desde la UI de vuelta al audit document:

- **Acción "View in Audit" por toast** — emite
  `OpenAuditRequested(Some(correlation_id))`. El workspace abre (o enfoca) el
  Audit document y aplica el filtro de correlation coincidente para que el
  usuario vea exactamente el único evento vinculado al toast.
- **Clic en el badge de error de la status bar** — emite
  `OpenAuditRequested(None)`. El workspace abre el Audit document con el filtro
  de user-error por defecto (`target = dbflux_ui::user_error` sobre una ventana
  de tiempo reciente) para que el usuario pueda explorar todos los fallos
  orientados al usuario recientes.

Ambos eventos fluyen a través de `AppStateEntity::request_open_audit` para que
el workspace se suscriba una sola vez.

Mapeo de severidad desde `EventSeverity`:
- `EventSeverity::Info` y `EventSeverity::Warn` — emitidos a nivel `WARN`;
  regulados (throttled) (token bucket de 5, 1 recarga cada 2 segundos, por
  severidad)
- `EventSeverity::Error` y `EventSeverity::Fatal` — emitidos a nivel `ERROR`;
  evitan el throttle

### Activar el puente

El puente se activa compilando `dbflux_core` con el feature `tracing-bridge`
(activado por defecto para `dbflux`, `dbflux_mcp_server`). Llama a
`init_tracing(BridgeConfig { .. })` una vez al arrancar el proceso:

```rust
use dbflux_core::observability::tracing_bridge::{init_tracing, BridgeConfig, FmtWriter};

let handle = init_tracing(BridgeConfig {
    include_audit_layer: true,
    fmt_writer: FmtWriter::Stderr,
    env_filter_default: "info",
    ..BridgeConfig::default()
})?;

// Later, after AuditService is ready:
handle.install_sink(Arc::new(audit_service));
```

`dbflux_driver_host` usa `include_audit_layer: false` porque los procesos de
driver host son efímeros y no tienen acceso a la base de datos SQLite de audit.

### Archivos clave

| Archivo                                                                                | Rol                                                                 |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `crates/dbflux_core/src/observability/tracing_bridge/mod.rs`                           | `init_tracing`, `BridgeHandle`, `BridgeConfig`, `LevelCode`         |
| `crates/dbflux_core/src/observability/tracing_bridge/layer.rs`                         | `AuditLayer`, `AuditFieldVisitor`, level gate                       |
| `crates/dbflux_core/src/observability/tracing_bridge/category.rs`                      | `PREFIX_CATEGORY_MAP`, `resolve_category`, `BRIDGE_INTERNAL_TARGET` |
| `crates/dbflux_storage/src/migrations/mod_014_audit_settings_log_capture_min_level.rs` | Añade la columna `log_capture_min_level` a `cfg_audit_settings`     |

## Emisión externa de audit (drivers RPC y auth providers)

Los drivers RPC externos (protocolo v1.2+) y los auth providers (protocolo
v1.3+) pueden emitir audit events de vuelta al host como frames de respuesta
intermedios. El host aplica una sanitización estricta antes de escribir en
`aud_audit_events`.

### Política controlada por el host

El host posee todos los campos de identidad, correlation y rate-limiting. Un
servicio externo nunca puede falsificar su propia identidad ni reclamar una
audit category para la que no tiene permiso.

| Campo            | Origen                                                                                                                    |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `actor_type`     | Siempre `ExternalDriver` o `ExternalAuthProvider`                                                                         |
| `source_id`      | Siempre `ExternalDriver` / `ExternalAuthProvider` con el `socket_id` registrado                                           |
| `actor_id`       | Siempre `rpc:<socket_id>`                                                                                                 |
| `connection_id`  | Provisto por el host desde el contexto de sesión (puede ser `None`)                                                       |
| `database_name`  | Provisto por el host desde el contexto de sesión (puede ser `None`)                                                       |
| `driver_id`      | Siempre `rpc:<socket_id>`                                                                                                 |
| `correlation_id` | Generado por el host; uno por sesión para drivers, uno por request para auth providers                                    |
| `ts_ms`          | Suministrado por el servicio, pero limitado si la desviación respecto al reloj de pared del host supera los cinco minutos |

`correlation_id` está garantizado estructuralmente a ser generado por el host
porque `AuditEventEmitDto` (el tipo de payload IPC) no tiene ningún campo
`correlation_id`. Los servicios externos no pueden suministrar uno — el campo se
omitió intencionalmente del DTO en el momento del diseño (ADR-3) en lugar de
aceptarse y validarse después. Como resultado, el escenario "el DTO del driver
lleva un correlation_id falsificado y el host lo sobrescribe en tiempo de
ejecución" es imposible a nivel de tipos; el valor almacenado siempre lo produce
la lógica de asignación de correlation-id del host.

### Lista blanca de categorías

Los drivers pueden emitir eventos `Connection`, `Query` y `System`. Los auth
providers solo pueden emitir eventos `Connection`. Cualquier frame con una
categoría no permitida se descarta silenciosamente.

### Rate limiting

Cada servicio externo (por `socket_id`) está limitado a 100 eventos cada 60
segundos mediante un token-bucket. Los frames que superan el presupuesto se
descartan y se contabilizan en `AuditService::external_audit_dropped_count()`.

### Flags de opt-in

- **Drivers**: el driver debe incluir `DriverCapability::AuditEmit` en su
  respuesta hello (protocolo v1.2+). Los frames enviados por drivers que no
  anunciaron esta capacidad se descartan silenciosamente.
- **Auth providers**: el provider debe establecer `audit_emit_opt_in: true` en
  su respuesta hello (protocolo v1.3+). Los frames de providers que no optaron
  por esto se descartan silenciosamente.

### Campos obligatorios en cada frame emitido

Un `AuditEventEmitDto` emitido debe tener `action` y `summary` no vacíos. Los
frames que no cumplan esta comprobación se descartan silenciosamente.

### Mecanismo de transporte

Los frames emitidos llegan como frames intermedios `done=false` dentro de una
secuencia de respuesta normal. La capa de transporte (`RpcClient` en
`dbflux_driver_ipc`, `RpcAuthProvider::dispatch_request_loop` en `dbflux_ipc`)
los intercepta antes de que lleguen a quien hizo la llamada. Quien llama solo
llega a ver el frame terminal.

### Archivos clave

| Archivo                                                | Rol                                                                         |
| ------------------------------------------------------ | --------------------------------------------------------------------------- |
| `crates/dbflux_ipc/src/audit.rs`                       | `AuditEventEmitDto`, trait `ExternalAuditEmitter`, `ExternalAuditSource`    |
| `crates/dbflux_app/src/rpc_services/external_audit.rs` | `ExternalAuditSink`, rate limiter de token-bucket, pipeline de sanitización |
| `crates/dbflux_driver_ipc/src/transport.rs`            | `RpcClient::send_raw` intercepta los frames de emisión del driver           |
| `crates/dbflux_ipc/src/auth_provider_client.rs`        | `dispatch_request_loop` intercepta los frames de emisión del auth provider  |

## Arquitectura

```
[Service layers]
  |  emit EventRecord via EventSink trait
  v
AuditService              (dbflux_audit/src/lib.rs)
  |  validate → fingerprint query text → redact sensitive values → enforce size limit
  v
SqliteAuditStore          (dbflux_audit/src/store/sqlite.rs)
  |  delegates to AuditRepository
  v
AuditRepository           (dbflux_storage/src/repositories/audit.rs)
  |  inserts into aud_audit_events
  v
~/.local/share/dbflux/dbflux.db
```

Archivos clave:

| Archivo                                           | Rol                                           |
| ------------------------------------------------- | --------------------------------------------- |
| `crates/dbflux_core/src/observability/types.rs`   | `EventRecord`, todos los tipos enum           |
| `crates/dbflux_core/src/observability/actions.rs` | Constantes canónicas de cadenas de action     |
| `crates/dbflux_audit/src/lib.rs`                  | `AuditService` — validate, preprocess, record |
| `crates/dbflux_audit/src/query.rs`                | `AuditQueryFilter`                            |
| `crates/dbflux_audit/src/export.rs`               | Export a CSV/JSON (básico y extendido)        |
| `crates/dbflux_audit/src/redaction.rs`            | Lógica de redacción de valores sensibles      |
| `crates/dbflux_audit/src/purge.rs`                | Purga de eventos basada en retención          |
| `crates/dbflux_audit/src/store/sqlite.rs`         | Adaptador del store de SQLite                 |
| `crates/dbflux_storage/src/repositories/audit.rs` | `AuditRepository` + `AuditEventDto`           |
