# Guía de integración de DBFlux con IA + MCP

Esta guía explica cómo integrar agentes de IA con DBFlux a través del binario
standalone del servidor MCP.

Es intencionalmente explícita sobre qué está disponible hoy y qué sigue
pendiente, para que las integraciones no dependan de un comportamiento que no
está implementado.

## 1. Visión general de la arquitectura

DBFlux expone la funcionalidad de servidor MCP a través del subcomando `dbflux
mcp`, que habla el Model Context Protocol sobre stdio. Los clientes de IA
(Claude Desktop, Cursor, etc.) lanzan este binario como subproceso y se
comunican vía JSON-RPC 2.0, delimitado por saltos de línea.

```
AI Client (Claude Desktop / Cursor / any MCP client)
        |  stdio  (JSON-RPC 2.0, newline-delimited)
        v
  dbflux mcp                    ← integrated into main dbflux binary
        |
        +--  dbflux_mcp          governance, authorization, tool catalog
        +--  dbflux_core         profiles, config, driver traits
        +--  dbflux_driver_*     real database drivers
        +--  dbflux_policy       policy engine
        +--  dbflux_audit        audit trail (SQLite)
```

El servidor MCP y la app GUI de DBFlux son procesos independientes. Comparten la
misma base de datos SQLite unificada en `~/.local/share/dbflux/dbflux.db`
(profiles, governance, audit, history, sessions). La governance configurada en
la GUI (trusted clients, roles, policies, ajustes por conexión) es leída por el
servidor desde esa base de datos al arrancar. El flag `--config-dir` se acepta
por compatibilidad de CLI, pero no reubica la base de datos unificada;
governance y audit siempre leen desde `~/.local/share/dbflux/dbflux.db`.

## 2. Ejecutar el servidor MCP

### Build

```bash
# All drivers with MCP support (default)
cargo build -p dbflux --release

# SQLite only with MCP
cargo build -p dbflux --features sqlite,mcp --release

# Without MCP support (AI integration disabled)
cargo build -p dbflux --no-default-features --features sqlite,postgres,mysql,mongodb,redis,dynamodb,lua,aws --release
```

El servidor MCP está integrado en el binario principal `dbflux`.

### Uso

```
dbflux mcp --client-id <id> [--config-dir <path>]
```

| Flag                  | Descripción                                                                                                                                                                                                                                              |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--client-id <id>`    | Identidad de este cliente de IA. Debe coincidir con un trusted client registrado en la configuración de governance. **Obligatorio.**                                                                                                                     |
| `--config-dir <path>` | Se acepta por compatibilidad de CLI. La base de datos de governance/audit siempre se resuelve a la unificada `~/.local/share/dbflux/dbflux.db`; este flag no la reubica. Para entornos de test aislados, sobrescribe `HOME`/`XDG_DATA_HOME` en su lugar. |

### Configuración de Claude Desktop

Agrega esto a `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) o el equivalente en tu plataforma:

```json
{
  "mcpServers": {
    "dbflux": {
      "command": "/path/to/dbflux",
      "args": ["mcp", "--client-id", "claude-desktop"]
    }
  }
}
```

El valor de `client-id` debe coincidir con una entrada de trusted client que
hayas creado en la GUI de DBFlux, en **Settings → MCP → Clients**.

**Nota**: si compilaste DBFlux sin el feature `mcp` (`--no-default-features`),
el servidor MCP no estará disponible.

## 3. Modelo de governance (conceptos centrales)

Toda solicitud de IA se hace cumplir a través de todas estas capas, en orden:

1. **Trusted client**: la identidad del solicitante debe estar activa y
   registrada.
2. **Connection MCP gate**: la conexión objetivo debe tener MCP habilitado.
3. **Policy assignment**: el actor debe tener una asignación con scope en esa
   conexión.
4. **Tool + decisión por clase**: el tool ID debe estar listado en una policy
   asignada, y esa policy decide la clase de ejecución de la llamada como Allow,
   Ask o Deny (ver la sección 5).
5. **Approval path**: una decisión Ask encola la llamada como ejecución
   pendiente. Una persona la aprueba o la rechaza en DBFlux, y una llamada
   aprobada se ejecuta una vez cuando el agente la repite con los mismos
   argumentos.
6. **Audit trail**: cada decisión se añade a `aud_audit_events` en la base de
   datos SQLite unificada y es consultable/exportable. Ver `docs/AUDIT.md` para
   el esquema completo de eventos.

Las seis capas se ejecutan dentro del proceso del servidor en cada solicitud
`tools/call`. Ninguna puede saltarse desde el lado del cliente.

## 4. Superficie canónica de tools (v1)

| Grupo           | Tool ID                   | Clase                                  | Qué hace                                                                                                             |
| --------------- | ------------------------- | -------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Connection      | `list_connections`        | metadata                               | Enumera todas las conexiones de base de datos configuradas                                                           |
| Connection      | `connect`                 | metadata                               | Abre una sesión contra una conexión configurada. La respuesta informa `current_database` y las `databases` disponibles en el servidor cuando el driver tiene bases de datos; las demás herramientas aceptan un parámetro `database` para apuntar a otra |
| Connection      | `disconnect`              | metadata                               | Cierra una sesión abierta                                                                                            |
| Connection      | `get_connection_info`     | metadata                               | Obtiene las capabilities del driver y los metadatos de la conexión                                                   |
| Schema          | `list_databases`          | metadata                               | Lista todas las bases de datos accesibles en una conexión                                                            |
| Schema          | `list_schemas`            | metadata                               | Lista los schemas dentro de una base de datos                                                                        |
| Schema          | `list_tables`             | metadata                               | Lista tablas y vistas dentro de un schema. Con `names_only: true` devuelve los nombres como strings en lugar de un objeto por entrada |
| Schema          | `list_collections`        | metadata                               | Lista colecciones de MongoDB. Acepta `names_only` igual que `list_tables`                                            |
| Schema          | `describe_object`         | metadata                               | Obtiene las definiciones de columnas/campos e índices de una tabla                                                   |
| Read            | `select_data`             | read                                   | Ejecuta un SELECT estructurado contra una tabla o colección. Los `joins` con otras tablas se ejecutan en drivers que declaran soporte de joins; los documentales, clave-valor y demás drivers que no lo declaran devuelven un error explícito. La condición `on` solo acepta comparaciones entre columnas unidas por `AND`. Con joins se rechazan una tabla calificada con esquema en conexiones que descartan el esquema (SQLite, Turso), `$ilike` en drivers que no lo declaran y una dirección de `order_by` distinta de `asc` o `desc`. SQL Server también ejecuta los joins, con su límite de filas generado como `OFFSET … FETCH`; las pruebas de joins se ejecutan en SQLite |
| Read            | `count_records`           | read                                   | Devuelve un conteo de filas/documentos para un target                                                                |
| Read            | `aggregate_data`          | read                                   | Ejecuta un pipeline de agregación de solo lectura                                                                    |
| Read            | `explain_query`           | read                                   | Muestra el plan de ejecución de la query sin ejecutar la mutación objetivo                                           |
| Read            | `preview_mutation`        | read                                   | Devuelve un preview/plan de solo lectura para una write query. Siempre de solo lectura; la mutación nunca se ejecuta |
| Write           | `insert_record`           | write                                  | Inserta un único registro                                                                                            |
| Write           | `update_records`          | write                                  | Actualiza registros que coinciden con un filtro                                                                      |
| Write           | `upsert_record`           | write                                  | Inserta o actualiza un único registro por clave                                                                      |
| Write           | `delete_records`          | destructive                            | Elimina registros que coinciden con un filtro                                                                        |
| Destructive     | `truncate_table`          | destructive                            | Elimina todas las filas de una tabla                                                                                 |
| DDL             | `create_table`            | admin                                  | Crea una tabla                                                                                                       |
| DDL             | `alter_table`             | admin_safe / admin / admin_destructive | Altera una tabla; la clasificación se calcula según el tipo de cambio                                                |
| DDL             | `create_index`            | admin                                  | Crea un índice                                                                                                       |
| DDL Destructive | `drop_index`              | admin_destructive                      | Elimina un índice                                                                                                    |
| DDL             | `create_type`             | admin                                  | Crea un tipo definido por el usuario                                                                                 |
| DDL Destructive | `drop_table`              | admin_destructive                      | Elimina una tabla                                                                                                    |
| DDL Destructive | `drop_database`           | admin_destructive                      | Elimina una base de datos                                                                                            |
| Scripts         | `list_scripts`            | metadata                               | Lista los scripts guardados en el directorio de scripts (las carpetas externas no se exponen)                        |
| Scripts         | `get_script`              | read                                   | Obtiene el source de un script guardado específico                                                                   |
| Scripts         | `create_script`           | write                                  | Guarda un nuevo script en el directorio de scripts                                                                   |
| Scripts         | `update_script`           | write                                  | Sobrescribe un script guardado existente                                                                             |
| Scripts         | `delete_script`           | admin                                  | Elimina permanentemente un script                                                                                    |
| Scripts         | `execute_script`          | computed                               | Ejecuta un script guardado contra una conexión. La clasificación se deriva del cuerpo del script                     |
| Aprobación      | `request_execution`       | admin                                  | Encola una llamada para que la apruebe una persona. Una vez aprobada, llama a la tool con los mismos argumentos para ejecutarla una vez |
| Aprobación      | `list_pending_executions` | read                                   | Muestra todas las ejecuciones pendientes de aprobación                                                               |
| Aprobación      | `get_pending_execution`   | read                                   | Obtiene los detalles de una ejecución pendiente específica. Una rechazada devuelve `status: "rejected"` y el motivo |
| Aprobación      | `approve_execution`       | —                                      | Siempre se deniega por MCP. Una persona aprueba en DBFlux                                                            |
| Aprobación      | `reject_execution`        | —                                      | Siempre se deniega por MCP. Una persona rechaza en DBFlux                                                            |
| Auditoría       | `query_audit_logs`        | read                                   | Busca y filtra el audit trail                                                                                        |
| Auditoría       | `get_audit_entry`         | read                                   | Obtiene una entrada específica del audit log por ID                                                                  |
| Auditoría       | `export_audit_logs`       | read                                   | Descarga entradas del audit log como CSV o JSON                                                                      |

Cuando el driver hace fallar una llamada a `select_data`, `count_records`, `aggregate_data` o `describe_object` y la tabla o colección no figura en los metadatos de esquema de la base de datos o del esquema consultado, el error indica dónde se buscó el nombre y lista los nombres listados más parecidos. Dice "is not listed" (no figura), porque la tabla puede no existir o la conexión puede no tener acceso a ella. Si la llamada no pasó `database` y el servidor lista más de una base de datos, el error agrega que la tabla puede estar en otra. Para tablas, no para colecciones, `count_records`, `aggregate_data` y `select_data` donde no se revisa antes de ejecutarse (ver abajo) hacen lo mismo con una columna nombrada en `where` u `order_by`. Una sugerencia solo incluye los nombres que el cliente tiene permitido listar: los nombres de tablas requieren `list_tables`, los de columnas requieren `describe_object` y los datos de bases de datos requieren `list_databases`. El texto de error del driver se conserva al final. La búsqueda solo se ejecuta después de que el driver hace fallar la llamada, y los drivers que no exponen estos metadatos devuelven el error original.

`select_data`, en cambio, revisa sus columnas antes de ejecutarse en los motores que leen un identificador entre comillas desconocido como un texto: SQLite y Turso, donde una columna mal escrita no devuelve filas en lugar de un error. En una tabla relacional, y en toda llamada con `joins`, cada nombre de `columns`, `where` u `order_by` sin calificar o calificado con su tabla se compara con los metadatos de columnas de la tabla, sean cuales sean sus caracteres, y solo las letras ASCII se igualan sin distinguir mayúsculas, como hace SQLite. Una columna que no figura se rechaza con la misma sugerencia, seguida de "The query was not run.", y no se ejecuta nada. Los nombres de tablas y vistas se resuelven sin distinguir mayúsculas ASCII, y cuentan como listadas las columnas generadas, las columnas ocultas de las tablas virtuales y `rowid`, `oid` y `_rowid_` donde el motor los acepta. Sin `describe_object` el rechazo no nombra ninguna otra columna. La revisión se omite, y la llamada se ejecuta como antes, cuando el driver no tiene metadatos de columnas para la tabla, y no se revisan las rutas anidadas ni los nombres calificados con otra tabla, porque el motor los rechaza. Cuesta una consulta de columnas por cada tabla de la que la llamada nombra una columna. PostgreSQL, MySQL, MariaDB, SQL Server, ClickHouse y Redshift fallan ante una columna desconocida, así que sus llamadas se ejecutan sin revisión y un fallo recibe la sugerencia descrita arriba.

Una pseudocolumna que el driver declara para la tabla, como `rowid` de SQLite, `_rowid` de MySQL o `ctid` y `xmin` de PostgreSQL, cuenta como listada y nunca se sugiere. Una llamada sin `joins` cuyo `columns` nombra una se ejecuta como un SELECT generado, igual que un join, así que el valor se devuelve: PostgreSQL devuelve `ctid` como un texto del tipo `(0,1)` y sus otras columnas de sistema como enteros. Ese SELECT acepta menos que una llamada normal: solo los operadores de `where` que acepta un join, identificadores ASCII simples, cada columna una vez y `asc` o `desc` como dirección de orden. En los motores que no se revisan antes de ejecutar, encontrar la pseudocolumna cuesta una consulta de columnas, que solo se hace cuando una entrada de `columns` falta en las filas que leyó la llamada.

Tools diferidas (rechazadas explícitamente en tiempo de solicitud en v1):

- `estimate_query_cost`
- `get_execution_status`

## 5. Clases de ejecución

Las policies controlan las tools en dos niveles: el tool ID en sí y la
clasificación de ejecución. Una policy lista las tools que cubre y le da a cada
clase de ejecución una decisión:

| Decisión | Qué pasa con una llamada de esa clase                                                                   |
| -------- | ------------------------------------------------------------------------------------------------------- |
| Allow    | Se ejecuta de inmediato                                                                                 |
| Ask      | Espera a una persona: la llamada se encola como ejecución pendiente y solo se ejecuta después de aprobarse |
| Deny     | Se rechaza                                                                                              |

| Clase               | Qué cubre                                                                         |
| ------------------- | --------------------------------------------------------------------------------- |
| `metadata`          | Inspección de schema — listar bases de datos, tablas y describir objetos          |
| `read`              | Ejecutar queries de solo lectura, obtener datos y previews de solo lectura        |
| `write`             | Insertar, actualizar o ejecutar scripts que modifican datos                       |
| `destructive`       | DELETE, DROP, TRUNCATE y otras operaciones irreversibles                          |
| `admin_safe`        | Operaciones DDL seguras como cambios de schema aditivos y creación de índices     |
| `admin`             | Operaciones DDL riesgosas, export de audit y acciones privilegiadas               |
| `admin_destructive` | Operaciones admin irreversibles, como eliminar o truncar objetos de schema        |

`metadata` y `read` solo leen. Las otras cinco clases modifican datos o schema y
más abajo se llaman clases que modifican datos.

### Cómo se combinan las policies

Un actor puede tener varias policies en una conexión, directamente y a través de
roles. Solo participan las policies que listan la tool pedida, y entre ellas gana
la decisión más permisiva: Allow sobre Ask sobre Deny. Las policies otorgan
acceso; un Deny es la ausencia de un permiso, no un veto. Por eso, una policy que
pide aprobación para una clase no frena a un actor al que otra policy asignada ya
le permite ejecutar esa clase. Para que una clase espere aprobación, asegúrate de
que ninguna otra policy asignada al actor la permita.

### El flujo de aprobación

1. El agente llama a una tool cuya clase la policy decide como Ask. El servidor
   encola la llamada como ejecución pendiente, registra un evento de auditoría
   `mcp_authorize` con outcome `pending` y responde con un error JSON-RPC cuyos
   datos son `{"code": "approval_required", "status": "pending", "pending_id": "..."}`.
2. Una persona aprueba o rechaza la llamada en DBFlux (**Workspace → Pending
   Approvals**). El servidor y la app comparten la cola a través de
   `dbflux.db`, así que una llamada encolada por `dbflux mcp` aparece en la app.
3. El agente vuelve a llamar a la misma tool con los mismos argumentos. El
   servidor encuentra la aprobación que coincide con el actor, la conexión, la
   tool y los argumentos, la consume y ejecuta la llamada. El evento
   `mcp_authorize` de esa llamada tiene outcome `success` y nombra la aprobación
   en `details_json.pending_execution_id`.

Una aprobación ejecuta una llamada. Repetir la llamada otra vez encola una nueva
solicitud, igual que cambiar cualquier argumento. Una llamada rechazada nunca se
ejecuta. Una aprobación vence 24 horas después de encolarse la llamada.

Tras un rechazo, `get_pending_execution` devuelve `status: "rejected"` y un
campo `reason` con el texto que la persona escribió al rechazar, recortado y
limitado a 500 caracteres, o `null` si no escribió ninguno. El mismo motivo queda
registrado en el evento de auditoría `mcp_reject_execution`.

`request_execution` encola una llamada de forma explícita, con el mismo resultado
que llamar a la tool bajo Ask. `request_execution`, `list_pending_executions` y
`get_pending_execution` solo crean o leen entradas de la cola, así que bajo Ask
se ejecutan sin encolarse ellas mismas.

Los clientes MCP nunca pueden aprobar ni rechazar: `approve_execution` y
`reject_execution` se deniegan por MCP diga lo que diga la policy, con el código
de error `self_approval_forbidden`, y cada intento se audita. Solo una persona
resuelve las ejecuciones pendientes, en la UI de DBFlux.

## 6. Policies y roles integrados

Se incluyen tres policies y tres roles como built-ins inmutables. Siempre están
presentes sin importar qué esté persistido en disco, y no pueden eliminarse ni
modificarse.

### Policies integradas

La lectura se permite por defecto y cada clase que modifica datos que otorga un
built-in pide aprobación.

| ID                  | Allow          | Ask                                                      | Scope                                                                                                                              |
| ------------------- | -------------- | -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `builtin/read-only` | metadata, read | —                                                        | Todas las tools de discovery + schema; tools de query y preview de solo lectura; listado/get de scripts; tools de lectura de audit |
| `builtin/write`     | metadata, read | write                                                    | Todas las tools de solo lectura más los flujos de scripts con capacidad de write y de request/approval-submission                  |
| `builtin/admin`     | metadata, read | write, destructive, admin_safe, admin, admin_destructive | Todas las tools canónicas excepto `approve_execution` y `reject_execution`                                                         |

Las clases que un built-in no lista se deniegan.

### Policies creadas antes de que existiera Ask

Antes de la decisión Ask, una policy solo podía permitir una clase, así que
permitir una clase que modifica datos nunca fue una elección explícita de
ejecutarla sin aprobación. La migración de almacenamiento que introdujo Ask
(`034_cfg_tool_policy_approval_classes`) reescribe las policies personalizadas
existentes en consecuencia: una clase que modifica datos y estaba permitida pasa
a Ask, una clase `metadata` o `read` permitida sigue en Allow y una clase que no
estaba permitida sigue en Deny. Para que un agente vuelva a ejecutar llamadas que
modifican datos sin aprobación, elige **Permitir todo sin aprobación** en la
policy.

### Roles integrados

Hay tres roles integrados: `builtin/read-only`, `builtin/write` y
`builtin/admin`. A cada uno se le asigna la policy con el mismo ID.

Los built-ins se inyectan al arranque tanto en la app GUI (`AppState`) como en
el servidor MCP (a través de los loops `builtin_policies()` / `builtin_roles()`
en `dbflux_mcp_server::governance`). Nunca se escriben en disco. Cualquier
intento de eliminar un built-in devuelve un error.

Para la mayoría de las integraciones, asigna `builtin/read-only` para empezar y
escala a `builtin/write` o a una policy personalizada solo cuando el acceso de
escritura sea explícitamente necesario.

## 7. Configuración del operador en la GUI de DBFlux

Configura la governance en la GUI de DBFlux antes de arrancar el servidor MCP.

1. **Settings → MCP → pestaña Clients**
   - Registra cada agente de IA como trusted client (`client_id` estable, nombre
     legible, issuer opcional).
   - Marca los clients como activos. Los clients inactivos se deniegan en el
     primer gate de autorización.

2. **Settings → MCP → pestaña Roles**
   - Los roles integrados (`Read Only`, `Write`, `Admin`) aparecen arriba y no
     se pueden eliminar.
   - Crea roles personalizados combinando múltiples policies con el dropdown
     multi-select.

3. **Settings → MCP → pestaña Policies**
   - Las policies integradas aparecen arriba y no se pueden modificar.
   - Crea policies personalizadas eligiendo tools y decidiendo Allow, Ask o
     Deny para cada clase de ejecución. Con el teclado, `enter` en una fila de
     clase pasa a la decisión siguiente.
   - **Permitir todo sin aprobación** pone en Allow todas las clases que
     modifican datos. El agente podrá entonces ejecutar cualquier llamada que
     modifique datos, incluido `DROP DATABASE`, sin preguntar.

4. **Connection Manager → pestaña MCP**
   - Habilita MCP para la conexión objetivo.
   - Selecciona el actor (trusted client), el role y/o la policy para esta
     conexión desde los dropdowns ya poblados.

5. **Workspace → Pending Approvals**
   - Revisa y aprueba o rechaza las llamadas que una policy envió a aprobación.
     Es el único lugar donde se resuelven las ejecuciones pendientes.
   - Una llamada en espera también aparece en el centro de notificaciones de la
     campana de la barra de título, que lleva el badge de acento mientras haya
     una esperando. **Revisar** en su fila abre esta pestaña en esa llamada; el
     popover nunca aprueba ni rechaza.
   - `j` / `k` recorren las llamadas pendientes, `a` aprueba la seleccionada y
     `r` la rechaza. Una llamada aprobada se ejecuta cuando el agente la repite
     con los mismos argumentos. Cada decisión se escribe en el audit log.
   - El campo de motivo del pie se envía al agente al rechazar. Mientras tiene el
     foco, `r` y `a` escriben texto en lugar de decidir. Se vacía tras cada
     decisión.

6. **Workspace → Audit**
   - Filtra por actor/tool/decisión/rango de tiempo y exporta CSV/JSON.

El servidor MCP lee esta configuración desde disco al arrancar. Si cambias la
configuración de governance en la GUI mientras el servidor está corriendo,
reinícialo para que tome la nueva configuración.

## 8. Archivos y rutas persistidas

DBFlux persiste todo su estado en una única base de datos SQLite unificada y
unos pocos directorios de soporte. Las rutas se resuelven con `dirs` (`XDG_*` en
Linux, `~/Library` en macOS).

Valores por defecto típicos en Linux:

| Ruta                              | Contenido                                                                                                       |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `~/.local/share/dbflux/dbflux.db` | Base de datos unificada: profiles, auth, SSH tunnels, governance, audit events, history, sessions, estado de UI |
| `~/.local/share/dbflux/sessions/` | Archivos de scratch y shadow conservados para restaurar y recuperar la sesión |
| `~/.local/share/dbflux/scripts/`  | Directorio de scripts creados por el usuario                                                                    |

La base de datos `dbflux.db` contiene todas las tablas de dominio bajo schemas
con prefijo:

- `cfg_*` — config (profiles, auth, governance, services, hooks, drivers)
- `st_*` — state (sessions, query history, estado de UI, saved queries)
- `aud_audit_events` — audit log unificado (eventos MCP, eventos de query,
  conexiones, hooks, scripts)
- `sys_*` — system (migrations, tracking de legacy import)

Las policies y roles integrados se sintetizan al arrancar y nunca se escriben en
disco.

Importante para tests: no uses directorios reales del usuario. Pasa
`--config-dir` al binario o define `HOME`/`XDG_CONFIG_HOME`/`XDG_DATA_HOME` a
rutas temporales para ejecuciones aisladas. El helper
`dbflux_audit::temp_sqlite_path(name)` genera rutas aisladas para tests de
audit.

## 9. Patrón de integración en Rust

### En proceso (app GUI, `AppState`)

```rust
// Register a trusted client
state.upsert_mcp_trusted_client(TrustedClientDto {
    id: "agent-a".into(),
    name: "Agent A".into(),
    issuer: None,
    active: true,
})?;

// Assign a built-in role to the agent on a connection
state.save_mcp_connection_policy_assignment(ConnectionPolicyAssignmentDto {
    connection_id: connection_id.to_string(),
    assignments: vec![ConnectionPolicyAssignment {
        actor_id: "agent-a".into(),
        role_ids: vec!["builtin/read-only".into()],
        policy_ids: vec![],
    }],
})?;
```

### Comprobar IDs de built-ins antes de eliminar

```rust
if dbflux_mcp::is_builtin(id) {
    // built-ins cannot be modified or deleted
}
```

### Llamada de autorización (usada internamente por el servidor MCP)

```rust
use dbflux_mcp::server::authorization::{AuthorizationRequest, authorize_request};

let outcome = authorize_request(
    &amp;trusted_clients,
    &amp;policy_engine,
    &amp;audit_service,
    &amp;AuthorizationRequest {
        identity: RequestIdentity { client_id: "agent-a".into(), issuer: None },
        connection_id: connection_id.to_string(),
        tool_id: "select_data".to_string(),
        classification: ExecutionClassification::Read,
        mcp_enabled_for_connection: true,
        correlation_id: None,
    },
    now_epoch_ms(),
)?;

if !outcome.allowed {
    // deny_code and deny_reason explain why
}
```

`authorize_request` no tiene cola de aprobación: una decisión Ask vuelve como no
permitida con `deny_code == Some("approval_required")`. El servidor MCP llama en
su lugar a `McpRuntime::authorize_with_approval_mut`, que pasa los argumentos de
la llamada para que una decisión Ask se encole, o consume una aprobación que
coincide y deja ejecutar la llamada.

## 10. Checklist de integración

Antes de apuntar un cliente de IA al servidor MCP:

- [ ] `dbflux` compilado con soporte MCP (habilitado por defecto, o con
  `--features mcp`)
- [ ] Trusted client registrado y activo en la GUI de DBFlux
- [ ] `--client-id` pasado al binario coincide con el client registrado
- [ ] La conexión objetivo tiene MCP habilitado
- [ ] El actor tiene una policy assignment en esa conexión
- [ ] La policy cubre las tools que usará el agente
- [ ] Las clases que necesitan aprobación están en Ask, y alguien atiende
  Pending Approvals mientras el agente trabaja

## 11. Higiene de tests

Para evitar contaminar las máquinas de desarrollo durante los tests:

- Pasa `--config-dir` a un directorio temporal o define
  `HOME`/`XDG_CONFIG_HOME`/`XDG_DATA_HOME`.
- Usa rutas SQLite temporales para tests de audit.
- No leas/escribas `~/.config/dbflux` ni `~/.local/share/dbflux` en código de
  test.
- Las policies y roles integrados están disponibles sin ninguna configuración
  previa — no los insertes manualmente en fixtures de test.
- El helper `dbflux_audit::temp_sqlite_path(name)` genera una ruta aislada para
  cada test.

## 12. Solución de problemas

### El servidor se cierra inmediatamente

- Falta el argumento `--client-id`.
- El directorio de configuración es inaccesible o no se puede crear.

### La solicitud se deniega como no confiable

- Verifica que el client exista y esté activo en la lista de trusted clients.
- Verifica que `--client-id` coincida exactamente con el `id` registrado
  (sensible a mayúsculas/minúsculas).

### La solicitud se deniega porque la conexión no tiene MCP habilitado

- Habilita MCP en la configuración de governance de la conexión objetivo
  (Connection Manager → pestaña MCP).
- O define `mcp_enabled_by_default: true` en la configuración si quieres que
  todas las conexiones estén habilitadas.

### Policy denegada

- Confirma que el actor tiene una assignment en el scope de esa conexión.
- Confirma que el tool ID está en las tools permitidas de la policy asignada.
- Confirma que la policy decide la clase de ejecución como Allow o Ask, no Deny.
- Si usas `builtin/read-only`, las tools de write (`create_script`, etc.) quedan
  excluidas por diseño.

### La llamada responde con `approval_required`

- La policy decide la clase de la llamada como Ask. Aprueba la ejecución
  pendiente indicada por `pending_id` en **Workspace → Pending Approvals** y
  repite la llamada con los mismos argumentos.
- Repetir la llamada con otros argumentos encola una nueva solicitud en lugar de
  usar la aprobación.
- Un agente no puede aprobar sus propias llamadas: `approve_execution` y
  `reject_execution` siempre se deniegan por MCP (`self_approval_forbidden`).

### La exportación de audit no muestra eventos

- Verifica que los filtros (`actor_id`, `tool_id`, rango de tiempo, decisión) no
  sean demasiado restrictivos.
- `export_audit_logs` está clasificada con la clase de ejecución `read`.

### No se puede eliminar una policy o un role

- Los IDs integrados (`builtin/read-only`, `builtin/write`, `builtin/admin`) no
  se pueden eliminar.
- Crea una policy personalizada con un ID distinto si necesitas una variante
  modificable.

### La configuración cambió en la GUI pero el servidor sigue usando los valores anteriores

- Reinicia el proceso del servidor MCP. La governance se carga desde disco una
  sola vez, al arrancar.
