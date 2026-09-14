# Conceptos clave

Esta guía es el modelo mental resumido para contributors y usuarios avanzados.
Describe los contratos entre subsistemas; [Architecture](../ARCHITECTURE.md)
sigue siendo el mapa canónico y exhaustivo de los límites de crates y los
archivos clave.

## Modelo mental

```text
UI document
  -&gt; app orchestration (profiles, connections, policy, lifecycle)
    -&gt; dbflux_core contracts (metadata, capabilities, requests, values)
      -&gt; built-in driver or RPC-adapted driver
        -&gt; QueryResult -&gt; generic result views
        -&gt; EventRecord -&gt; audit sink
```

La dirección importante es hacia adentro: la presentación y los workflows
dependen de contratos, mientras que el comportamiento específico de cada driver
se queda detrás de esos contratos. El audit observa el trabajo a lo largo del
flujo en lugar de formar un camino de ejecución separado.

## Mapa de conceptos

| Concepto             | Qué es                                                                                                                   | Por qué importa                                                                                                                      | Profundizar                                                                                                               |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| Drivers              | Implementaciones de los contratos `DbDriver` y `Connection`.                                                             | Las bases de datos integradas y externas entran a la app por el mismo límite.                                                        | [Core traits](../crates/dbflux_core/src/core/traits.rs), [Driver Authoring](DRIVER_AUTHORING.md)                          |
| `DriverMetadata`     | La identidad declarativa de un driver: categoría, lenguaje, forms y descriptores de features detallados.                 | Los workflows genéricos pueden elegir presentación y comportamiento sin identificar un driver concreto.                              | [Metadata definition](../crates/dbflux_core/src/driver/capabilities.rs)                                                   |
| `DriverCapabilities` | Flags de feature declarados por un driver y expuestos por una conexión.                                                  | La UI habilita solo las operaciones soportadas en lugar de adivinar a partir de nombres de bases de datos.                           | [Capability flags](../crates/dbflux_core/src/driver/capabilities.rs)                                                      |
| Documents            | Panes con type erasure gestionados como tabs, con identidad y contratos de evento.                                       | Los tipos de document nuevos pueden participar sin extender un enum de document cerrado.                                             | [`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs), [`TabManager`](../crates/dbflux_ui_document/src/tab_manager.rs) |
| Query results        | Shapes estructurados, columnas, filas y valores retornados por las conexiones.                                           | Las tablas, árboles, vistas de texto, exports y charts genéricos consumen un único modelo de resultado.                              | [Result types](../crates/dbflux_core/src/query/types.rs), [`Value`](../crates/dbflux_core/src/core/value.rs)              |
| MCP governance       | Enforcement de trusted-client, connection, classification, policy, approval y audit alrededor de las herramientas de AI. | El acceso de agentes es explícito, acotado, revisable y observable.                                                                  | [AI + MCP Integration](MCP_AI_INTEGRATION.md)                                                                             |
| Audit                | El seam de observability transversal `EventRecord`/`EventSink`.                                                          | Las queries, el trabajo de lifecycle, los hooks, el governance y los servicios externos comparten un trail correlacionado.           | [Audit reference](AUDIT.md), [`EventSink`](../crates/dbflux_core/src/observability/source.rs)                             |
| Hooks                | Commands, scripts o Lua adjuntos a fases del lifecycle de conexión.                                                      | La configuración y limpieza del entorno se mantienen fuera de las implementaciones de driver, con comportamiento de fallo explícito. | [Hook contracts](../crates/dbflux_core/src/connection/hook.rs), [Settings & Hooks](SETTINGS.md#connection-hooks)          |
| RPC services         | Descriptores persistidos adaptados en el startup a drivers o auth providers.                                             | Las integraciones fuera de proceso se suman al runtime sin convertirse en casos especiales de la UI.                                 | [RPC config](RPC_SERVICES_CONFIG.md), [protocol](DRIVER_RPC_PROTOCOL.md)                                                  |

## Los drivers son contratos, no casos de UI

`DbDriver` crea y describe una integración de base de datos; `Connection` expone
operaciones sobre una conexión activa. Sus contratos actuales viven en
[`core/traits.rs`](../crates/dbflux_core/src/core/traits.rs). Los crates
integrados los implementan directamente, mientras que los drivers externos se
adaptan vía RPC.

La regla de desacoplamiento es estricta: el código de UI y de app workflow se
adapta a través de metadata, capabilities y contratos genéricos, nunca de IDs de
driver concretos. Si una feature requiere `if driver == "postgres"` en código de
presentación o workflow, la abstracción que falta pertenece a la metadata, a una
capability, o a un contrato del core.

### Metadata y capabilities

[`DriverMetadata`](../crates/dbflux_core/src/driver/capabilities.rs) describe
qué es un driver: identidad de display, `DatabaseCategory`, lenguaje de query,
descriptores de sintaxis y operación, límites, y otros inputs genéricos de
presentación. `DriverCapabilities` declara qué features amplios soporta. Una
conexión expone la misma metadata y capabilities para que los callers no
necesiten su objeto driver de origen.

Usa la metadata para elegir un modo genérico y las capabilities para hacer gate
de una operación. No infieras soporte a partir de una clave de driver, un ícono,
una cadena de type-name nativo, o una lista mantenida en la UI.

## Los documents son polimorfismo abierto

[`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs) es el seam de
polimorfismo de document. Hace type erasure de cada entity concreta de GPUI
detrás de closures para renderizado, focus, commands, metadata, comportamiento
de lifecycle, deduplicación y subscription. El workspace por lo tanto no modela
los documents como un enum cerrado de tipos de document concretos.

[`DocumentKey`](../crates/dbflux_ui_document/src/dedup.rs) expresa identidad de
open-document. Cada pane decide si coincide con una key, y
[`TabManager`](../crates/dbflux_ui_document/src/tab_manager.rs) usa ese contrato
para enfocar un tab existente en lugar de abrir uno duplicado.

Los documents emiten
[`DocumentEvent`](../crates/dbflux_ui_document/src/handle.rs). El tab manager y
el workspace traducen esos eventos en acciones cross-document sin alcanzar una
implementación de document concreta. Agrega un comportamiento de pane en este
seam en lugar de agregar matching de tipo concreto al código del workspace.

## Los query results son datos estructurados

El límite de resultado actual es
[`QueryResult`](../crates/dbflux_core/src/query/types.rs): un `QueryResultShape`
declarado, entradas de `ColumnMeta`, filas de `Value` del core, texto o bytes
opcionales, timing de ejecución, y posibles result sets adicionales.
[`Value`](../crates/dbflux_core/src/core/value.rs) preserva valores relacionales
y de document sin reducir todo a JSON o cadenas de display.

Los valores de resultado estructurados y la metadata de columna alimentan vistas
genéricas. En particular, `ColumnMeta.kind` lleva información de tipo semántica
como timestamp, float, integer, text, o unknown. Los charts y otros consumidores
usan ese kind semántico; no deben inspeccionar `ColumnMeta.type_name` ni
ramificarse por identidad de driver.

## El governance de MCP envuelve la ejecución

El proceso de MCP autoriza requests a través de identidad de trusted-client, el
gate de MCP por conexión, la clasificación de ejecución, los roles y policies
asignados, y approval cuando se requiere. Las decisiones y ejecuciones quedan
auditadas. Ver el [modelo de
governance](MCP_AI_INTEGRATION.md#3-governance-model-core-concepts), la
[autorización de
`dbflux_mcp`](../crates/dbflux_mcp/src/server/authorization.rs), el [policy
engine](../crates/dbflux_policy/src/engine.rs), y el [approval
service](../crates/dbflux_approval/src/service.rs).

Las propiedades de seguridad son parte del límite:

- `preview_mutation` es read-governed y produce un plan de solo lectura; no
  ejecuta la mutación. La implementación rechaza cualquier query de preview
  generada por el driver que no esté clasificada como metadata/read ([query
  tool](../crates/dbflux_mcp_server/src/tools/query.rs)).
- `select_data` actualmente rechaza los joins solicitados en lugar de ignorarlos
  en silencio ([read tool](../crates/dbflux_mcp_server/src/tools/read.rs)).
- El preview de mutation no es un surface de DDL preview. Las operaciones DDL
  son herramientas gobernadas separadas; no se expone ninguna herramienta de DDL
  preview en el [tool catalog](../crates/dbflux_mcp/src/tool_catalog.rs) actual.

Mantén las decisiones de classification, policy, approval y audit en el límite
de governance. Un handler no debe debilitarlas porque un driver subyacente pueda
realizar la operación.

## El audit es el seam de observability

Los servicios emiten valores canónicos de
[`EventRecord`](../crates/dbflux_core/src/observability/types.rs) a través de
[`EventSink`](../crates/dbflux_core/src/observability/source.rs). El record
lleva actor, source, category, outcome, target context, details, y campos de
correlación; el sink es dueño del comportamiento de validación y almacenamiento.

Esto hace que el audit sea transversal: la ejecución de queries, el lifecycle de
conexión, los hooks, las decisiones de MCP, la configuración, y los servicios
RPC externos se pueden observar sin acoplar su lógica de dominio a la
implementación de SQLite. Usa la [referencia de audit](AUDIT.md) para el schema,
la validación, la redacción, la retención, y los detalles del tracing bridge.

## Los hooks rodean el lifecycle de conexión

[`ConnectionHook`](../crates/dbflux_core/src/connection/hook.rs) define trabajo
de command, script, o Lua en `PreConnect`, `PostConnect`, `PreDisconnect`, y
`PostDisconnect`. Los hooks pertenecen a la orquestación alrededor de una
conexión, no al contrato de query del driver de base de datos.

La failure policy es explícita: `Disconnect` aborta la fase, `Warn` continúa con
una advertencia visible, e `Ignore` continúa mientras loguea el fallo. La
ejecución puede ser bloqueante o detached, con controles de timeout, entorno y
señal de ready. Ver [Settings & Connection Hooks](SETTINGS.md#connection-hooks)
para configuración y detalles de seguridad.

## Los RPC services son descriptores de runtime

Los RPC services son descriptores persistidos de lanzamiento y compatibilidad
clasificados como `Driver` o `AuthProvider`. En el startup,
[`dbflux_app::rpc_services`](../crates/dbflux_app/src/rpc_services/) descubre
descriptores, valida y hace probe del protocolo apropiado, y luego adapta los
servicios exitosos al registro de drivers o de auth providers. Que una familia
de servicio falle no redefine a la otra.

Las keys del registro de driver externo se mantienen como `rpc:<socket_id>`. Los
auth providers usan su identidad de provider y nunca aparecen como drivers de
base de datos. La metadata de runtime para un driver externo viene de su
handshake, no de condicionales de UI. Ver [RPC Services
Config](RPC_SERVICES_CONFIG.md) para persistencia y [Driver RPC
Protocol](DRIVER_RPC_PROTOCOL.md) para transport, negotiation, lifecycle, y
emisión de audit.

## Dónde hacer un cambio

| Necesitas cambiar…                                              | Empieza en…                                                                                                              | Regla de reconocimiento                                                                       |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| Una operación de base de datos disponible para toda integración | [`DbDriver`/`Connection`](../crates/dbflux_core/src/core/traits.rs)                                                      | Define un contrato genérico antes de implementar drivers.                                     |
| Si una feature genérica se muestra o se permite                 | [Metadata y capabilities](../crates/dbflux_core/src/driver/capabilities.rs)                                              | Declara soporte; nunca identifiques el driver en código de UI/workflow.                       |
| Renderizado de resultado o comportamiento de tipo semántico     | [Query result types](../crates/dbflux_core/src/query/types.rs)                                                           | Consume shape, values, y `ColumnMeta.kind`.                                                   |
| Un document nuevo del workspace                                 | [`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs) y [`DocumentEvent`](../crates/dbflux_ui_document/src/handle.rs) | Implementa el seam de open pane y una dedup key; no extiendas una union de document concreta. |
| Una herramienta de AI o regla de ejecución                      | [MCP integration](MCP_AI_INTEGRATION.md) y [authorization](../crates/dbflux_mcp/src/server/authorization.rs)             | Preserva el orden de classification, policy, approval, y audit.                               |
| Una acción observable de dominio                                | [`EventRecord` y `EventSink`](../crates/dbflux_core/src/observability/types.rs)                                          | Emite a través del seam; mantén los detalles de almacenamiento fuera del código de dominio.   |
| Automatización de setup o limpieza de conexión                  | [Hook contract](../crates/dbflux_core/src/connection/hook.rs)                                                            | Elige una fase de lifecycle y un modo de fallo explícito.                                     |
| Un driver o auth provider fuera de proceso                      | [RPC services](RPC_SERVICES_CONFIG.md)                                                                                   | Persiste un descriptor y adapta a través de `dbflux_app::rpc_services`.                       |

Continúa con [Architecture](../ARCHITECTURE.md) para los límites canónicos de
crates y los archivos clave, o con [Driver Authoring](DRIVER_AUTHORING.md) para
implementar un driver integrado o externo.
