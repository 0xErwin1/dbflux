# Especificación del protocolo Driver RPC

Este documento define cómo DBFlux descubre, lanza y se comunica con servicios
RPC a través de IPC local.

DBFlux ahora activa dos familias de servicios en runtime:

- `RpcServiceKind::Driver` -> drivers de base de datos en runtime
- `RpcServiceKind::AuthProvider` -> registros de auth-provider en runtime, en la
  app y en el servidor MCP

## Fuente de verdad

Para los servicios de driver activos, **el servicio es la fuente de verdad**
para:

- el tipo de driver (`DbKind`)
- los metadatos del driver (`DriverMetadataDto`: nombre, icon, category,
  capabilities, query language, etc.)
- la definición del formulario de conexión (`DriverFormDefDto`)

DBFlux guarda la configuración de lanzamiento en su services config respaldada
por SQLite. Los servicios RPC se crean y editan desde **Settings → RPC
Services**.

## Modelo de integración

Al arrancar la app, DBFlux carga los servicios RPC configurados desde
`~/.local/share/dbflux/dbflux.db`, y para cada servicio:

1. descubre el descriptor de servicio persistido, incluyendo `RpcServiceKind`
2. bifurca según `kind`
3. asegura que el servicio esté corriendo (lo inicia si es necesario)
4. realiza el handshake `Hello` específico de la familia
5. lee los metadatos en runtime desde el servicio
6. registra el servicio en runtime adaptado en el registro en memoria
   correspondiente

Si algún paso falla, ese servicio se omite sin abortar el arranque. Los fallos
de driver no rompen los auth providers, y los fallos de auth-provider no rompen
los drivers.

Comportamiento importante:

- La configuración de servicios se lee al arrancar. Reinicia DBFlux después de
  cambiar la configuración de servicios RPC.
- `socket_id` se usa tal cual (DBFlux no lo reescribe).
- La clave interna del registro es `rpc:<socket_id>`.

## Transporte

DBFlux usa sockets locales vía `interprocess`:

- **Linux**: sockets Unix de namespace abstracto (`\0name`)
- **macOS**: sockets Unix en `/tmp/`
- **Windows**: named pipes (`\\.\pipe\...`)

Los mensajes se enmarcan como:

- longitud little-endian de 4 bytes (`u32`)
- payload en bincode

Tamaño máximo de mensaje: `16 MiB`.

La limpieza de sockets es automática al salir/soltar el proceso (provista por
`interprocess`).

## Configuración en runtime

Almacenamiento primario: `~/.local/share/dbflux/dbflux.db` (`cfg_services`,
`cfg_service_args`, `cfg_service_env`)

UI de settings: **Settings → RPC Services**

Notas:

- `socket_id` es obligatorio.
- `kind` soporta `driver` y `auth_provider`.
- `command` es opcional.
  - Si `command` se omite y `args` está vacío, DBFlux espera que el servicio ya
    esté corriendo.
  - Para `driver`, si `command` se omite y `args` no está vacío, DBFlux lanza
    `dbflux-driver-host`.
  - Para `auth_provider`, el lanzamiento gestionado requiere un `command`
    explícito; DBFlux no asume un binario host por defecto.
- `args`, `env` y `startup_timeout_ms` son opcionales.
- DBFlux deriva una clave interna de registro de drivers como `rpc:<socket_id>`.
- Solo los servicios `driver` se registran como drivers de base de datos.
- Los servicios `auth_provider` se registran únicamente en los registros de
  auth-provider y nunca reciben una identidad de driver `rpc:<socket_id>`.

## Contrato de handshake

DBFlux conecta y envía `Hello` primero.

La familia activa de la API driver RPC es `driver_rpc`. En el transporte driver
RPC dedicado actual, esa familia es implícita en el propio protocolo en lugar de
transmitirse por el cable durante `Hello`. La compatibilidad se hace cumplir
mediante el endpoint driver RPC más la versión mayor de protocolo seleccionada;
las versiones menores son aditivas y se negocian de forma determinista dentro de
esa línea mayor.

Solicitud del cliente:

```rust
DriverRequestBody::Hello(DriverHelloRequest {
    client_name: "dbflux_driver_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![
        ProtocolVersion::new(1, 0),
        ProtocolVersion::new(1, 1),
        ProtocolVersion::new(1, 2),
    ],
    requested_capabilities: vec![
        DriverCapability::Cancellation,
        DriverCapability::ChunkedResults,
        DriverCapability::SchemaIntrospection,
        DriverCapability::MultiDatabase,
    ],
})
```

La respuesta del servidor debe incluir:

- `selected_version`
- `capabilities`
- `driver_kind`
- `driver_metadata`
- `form_definition`

Ejemplo:

```rust
DriverResponseBody::Hello(DriverHelloResponse {
    server_name: "my-driver".to_string(),
    server_version: "1.0.0".to_string(),
    selected_version: DRIVER_RPC_VERSION,
    capabilities: vec![DriverCapability::SchemaIntrospection],
    driver_kind: DbKind::SQLite,
    driver_metadata: DriverMetadataDto {
        id: "my-driver".to_string(),
        display_name: "My Driver".to_string(),
        description: "External RPC driver".to_string(),
        category: DatabaseCategory::Relational,
        query_language: QueryLanguageDto::Sql,
        capabilities: DriverCapabilities::RELATIONAL_BASE.bits(),
        default_port: None,
        uri_scheme: "mydriver".to_string(),
        icon: Icon::Database,
    },
    form_definition: DriverFormDefDto {
        tabs: vec![
            // ...
        ],
    },
})
```

Si se solapan varias versiones menores compatibles, el host debe seleccionar la
versión menor mutua más alta.

Si no existe ninguna versión compatible, se devuelve
`DriverRpcErrorCode::VersionMismatch`.

Después de `Hello`, cada envelope de request y response debe usar la
`selected_version` negociada. Un peer que reciba un envelope post-handshake con
una versión distinta debe rechazarlo como version mismatch.

Límite de validación actual:

- DBFlux persiste metadatos de API family/version por servicio, para discovery y
  futuros seams en runtime.
- El handshake de driver en vivo actualmente valida las versiones de protocolo
  negociadas, pero no transmite ni revalida por separado el string de API family
  en el cable, porque el transporte driver RPC ya es específico de esa familia.

### Emisión de audit desde drivers (v1.2+)

Un driver que anuncia `DriverCapability::AuditEmit` (driver RPC ≥ 1.2) puede
escribir en el audit log del host enviando frames intermedios `EmitAuditEvent`
(`done=false`) durante cualquier ciclo de request/response. El host sanitiza
cada evento antes de persistirlo en `aud_audit_events`.

Categorías permitidas: `Connection`, `Query`, `System`. Todas las demás
categorías se descartan silenciosamente.

El host sobrescribe los campos de identidad (`actor_type` → `ExternalDriver`,
`actor_id`, `source_id`, `driver_id`, `correlation_id`) y el contexto de
conexión desde `AppState`, y trunca `details_json` al límite configurado. El
rate limiting se comparte con los auth providers: 100 eventos por 60 segundos
por `socket_id`; los eventos que exceden ese límite se descartan sin generar
error en la sesión. Los peers que negocian por debajo de v1.2 u omiten la
capability permanecen silenciosos. Ver [Audit § emisión de audit
externa](AUDIT.md) para el contrato completo de sanitización.

## Contrato RPC de auth-provider

La familia activa de la API auth-provider RPC es `auth_provider_rpc` en `1.3`.

DBFlux usa los metadatos persistidos `api_family` / `api_major` como preflight
de arranque. Las filas compatibles luego negocian la versión menor mutua más
alta durante `Hello`.

Solicitud del cliente:

```rust
AuthProviderRequestBody::Hello(AuthProviderHelloRequest {
    client_name: "dbflux_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![
        ProtocolVersion::new(1, 3),
        ProtocolVersion::new(1, 2),
        ProtocolVersion::new(1, 1),
        ProtocolVersion::new(1, 0),
    ],
    auth_token: Some("<token>".to_string()),
})
```

La respuesta del servidor debe incluir:

- `selected_version`
- `provider_id`
- `display_name`
- `form_definition`

La respuesta `Hello` de v1.2 lleva adicionalmente `secret_dependency_opt_in`
(`bool`), que declara si el provider opta por recibir valores de campos secretos
dentro de los dependency maps para las búsquedas de opciones dinámicas. Cuando
es `false` (default), DBFlux elimina los valores secretos de los dependency maps
antes de reenviar las solicitudes `FetchDynamicOptions`.

La respuesta `Hello` de v1.3 lleva adicionalmente `audit_emit_opt_in` (`bool`).
Establécelo en `true` para habilitar la emisión de audit events (ver abajo). El
default es `false`.

Flujo de request/response soportado:

| Request               | Response                            | Propósito                                                                                   |
| --------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------- |
| `Hello`               | `Hello`                             | negociación de protocolo + identidad del provider                                           |
| `ValidateSession`     | `SessionState`                      | validar el estado de auth cacheado                                                          |
| `Login`               | `LoginUrlProgress?` + `LoginResult` | URL de verificación opcional + resultado de login terminal                                  |
| `ResolveCredentials`  | `Credentials`                       | resolver los campos de credenciales en runtime                                              |
| `FetchDynamicOptions` | `DynamicOptions`                    | resolver opciones de dropdown dinámicas para un campo de formulario `DynamicSelect` (v1.2+) |
| (cualquier request)   | `EmitAuditEvent` (intermedio)       | emisión de audit event (v1.3+)                                                              |

Notas:

- `Login` puede emitir cero o un evento `LoginUrlProgress` antes de
  `LoginResult`.
- Si no se envía ningún evento de progreso, DBFlux trata el callback de la URL
  de verificación como `None`.
- `FetchDynamicOptions` solo está disponible cuando la versión negociada es al
  menos `1.2`. Los providers que negocian por debajo de v1.2 reciben del host un
  resultado permanente de "not supported" sin round-trip de IPC.
- `detect_importable_profiles`, los hooks de write-back de perfil y el registro
  de value-provider específico de cada provider quedan intencionalmente fuera
  del alcance del contrato RPC en este cambio.
- Los fallos en runtime de auth-provider se manejan a través del manejo
  existente de `DbError` y no abortan el arranque.

### Emisión de audit desde auth providers (v1.3+)

Los auth providers que negocian v1.3+ y establecen `audit_emit_opt_in: true`
pueden enviar frames intermedios `EmitAuditEvent` (`done=false`) durante
cualquier ciclo de request/response. El host los sanitiza y los escribe en
`aud_audit_events`.

Categoría permitida: solo `Connection`. Todas las demás categorías se descartan
silenciosamente.

El payload `AuditEventEmitDto` sigue la misma estructura que los frames de emit
de driver. El host sobrescribe los campos de identidad (`actor_type`,
`actor_id`, `source_id`, `driver_id`, `correlation_id`). El rate limiting se
comparte con los drivers: 100 eventos por 60 segundos por `socket_id`.

## Contrato del formulario

El formulario de conexión mostrado en DBFlux se construye a partir de
`form_definition`, devuelto en `Hello`.

- El servicio define fields/tabs/sections.
- DBFlux valida los campos obligatorios en la UI.
- Al conectar/guardar, DBFlux envía los valores recolectados a través de
  `DbConfig::External.values` en el JSON de profile de `OpenSession`.

Si `form_definition.tabs` está vacío, el formulario de conexión no mostrará
ningún input específico del driver.

## Ciclo de vida de la sesión

1. `Hello`
2. `OpenSession`
3. operaciones de request/response
4. `CloseSession`

`OpenSession` siempre devuelve `SessionOpened` con metadatos. Mantén esto
consistente con los metadatos de `Hello`.

DBFlux envía el JSON del profile guardado a `OpenSession`. Para drivers
externos, la configuración del profile es:

```rust
DbConfig::External {
    kind: DbKind,
    values: HashMap<String, String>,
}
```

`values` contiene los valores de campo recolectados desde tu `form_definition`.

El servicio debe parsear `profile_json`, esperar `DbConfig::External`, y validar
de nuevo los campos obligatorios del lado del servidor.

## Resumen de request/response

| Request         | Response        | Propósito                                       |
| --------------- | --------------- | ----------------------------------------------- |
| `Hello`         | `Hello`         | negociación de protocolo + identidad del driver |
| `OpenSession`   | `SessionOpened` | abrir conexión/sesión                           |
| `CloseSession`  | `SessionClosed` | cerrar sesión                                   |
| `Ping`          | `Pong`          | liveness                                        |
| `Execute`       | `ExecuteResult` | ejecución de query                              |
| `Schema`        | `Schema`        | snapshot de schema                              |
| `ListDatabases` | `Databases`     | listado de bases de datos                       |

El protocolo también soporta operaciones de browse, CRUD, key-value y generación
de código. Ver `crates/dbflux_ipc/src/driver_protocol.rs` para el conjunto
completo de enums.

## Emisión de audit desde drivers (v1.2+)

Los drivers que negocian la versión de protocolo v1.2 o superior pueden emitir
audit events de vuelta al host como frames de respuesta intermedios
(`done=false`). El host los sanitiza, aplica rate limiting y los escribe en
`aud_audit_events`.

### Habilitar la opción

Incluye `DriverCapability::AuditEmit` en la lista `capabilities` de tu respuesta
`Hello`. Los drivers que no anuncian esta capability tendrán cualquier frame
`EmitAuditEvent` descartado silenciosamente por el host.

### Enviar un frame de audit

Emite un `DriverResponseEnvelope` con `done = false` y `body =
DriverResponseBody::EmitAuditEvent(AuditEventEmitDto { .. })` en cualquier punto
durante una request, antes de la respuesta terminal:

```rust
DriverResponseEnvelope {
    protocol_version: negotiated_version,
    request_id: request.request_id,
    session_id: request.session_id,
    done: false,
    body: DriverResponseBody::EmitAuditEvent(AuditEventEmitDto {
        ts_ms: chrono::Utc::now().timestamp_millis(),
        level: EventSeverityDto::Info,
        category: EventCategoryDto::Connection,
        action: "session.open".to_string(),
        outcome: EventOutcomeDto::Success,
        summary: "Database session opened".to_string(),
        object_type: None,
        object_id: None,
        duration_ms: Some(42),
        error_code: None,
        error_message: None,
        details_json: None,
    }),
}
```

Luego envía la respuesta terminal como de costumbre.

### Qué provee el host

El host siempre sobrescribe estos campos; no los incluyas en el DTO (están
intencionalmente ausentes de `AuditEventEmitDto`):

- `actor_type`, `actor_id`, `source_id`, `driver_id` — siempre fijados a
  `ExternalDriver` / `rpc:<socket_id>`
- `connection_id`, `database_name` — resueltos desde el contexto de sesión
  activo
- `correlation_id` — uno por sesión, generado por el host

### Categorías permitidas

Los drivers pueden emitir eventos `Connection`, `Query` y `System`. Todas las
demás categorías se descartan silenciosamente.

### Rate limit

100 eventos por 60 segundos por `socket_id`. Los frames que exceden el límite se
descartan y se cuentan en `AuditService::external_audit_dropped_count()`.

## Manejo de errores

Devuelve errores estructurados a través de
`DriverResponseBody::Error(DriverRpcError { ... })`.

Códigos comunes:

- `InvalidRequest`
- `UnsupportedMethod`
- `VersionMismatch`
- `SessionNotFound`
- `Timeout`
- `Cancelled`
- `Transport`
- `Driver`
- `Internal`

Usa `InvalidRequest` para profiles/valores de formulario malformados y
`UnsupportedMethod` para métodos intencionalmente no implementados. El
auth-provider RPC usa el conjunto paralelo `AuthProviderRpcErrorCode` con el
mismo significado operativo (`VersionMismatch`, `UnsupportedMethod`, `Timeout`,
`Transport`, etc.).

## Ciclo de vida del proceso y limpieza

Cuando DBFlux inicia un proceso de servicio por sí mismo (vía `command` o el
comando de host por defecto soportado), ese proceso se rastrea como un host
gestionado.

Al cerrar DBFlux:

- todos los hosts gestionados rastreados se matan (`kill + wait`)
- los hosts iniciados manualmente fuera de DBFlux no se rastrean y no se matan

Esto garantiza que DBFlux solo limpia los procesos que posee.

Si un host gestionado sale antes de tiempo o excede el timeout antes de que el
socket esté listo, DBFlux reporta el id del servicio junto con una cola acotada
del stdout/stderr reciente para ayudar en el diagnóstico.

## Checklist mínimo de implementación

Tu servicio debería:

1. hacer bind del socket vía `interprocess`
2. manejar `Hello` y devolver metadata/kind
3. devolver una definición de formulario en `Hello`
4. manejar `OpenSession`/`CloseSession`
5. implementar al menos una operación útil (`Execute`)
6. devolver `UnsupportedMethod` para operaciones no implementadas

Recomendado:

7. validar `DbConfig::External.values` en `OpenSession`
8. devolver errores `InvalidRequest` claros para valores de formulario
   faltantes/inválidos
9. mantener consistentes los metadatos de `Hello` y los de `SessionOpened`
10. sellar cada envelope post-`Hello` con la versión negociada en lugar de
    asumir la última constante

## Ejemplo funcional en este repositorio

Usa:

- `examples/custom_driver/src/main.rs`
- `examples/custom_driver/README.md`
- `examples/custom_auth_provider/src/main.rs`
- `examples/custom_auth_provider/README.md`

Esos ejemplos son compatibles con el modelo de integración de servicio de driver
activo actual.

Ruta de prueba rápida:

1. agrega un nuevo servicio **Driver** en **Settings → RPC Services**
2. apunta `command` a tu binario de ejemplo compilado
3. establece `args` a `--socket <your-socket-id>`
4. reinicia DBFlux
5. crea ya sea una conexión (ejemplo de driver) o un perfil de auth (ejemplo de
   auth-provider) a través de los formularios de UI expuestos por el servicio

## Referencias

- `crates/dbflux_ipc/src/driver_protocol.rs`
- `crates/dbflux_driver_ipc/src/transport.rs`
- `crates/dbflux_driver_host/src/main.rs`
- `crates/dbflux/src/app.rs`
- `crates/dbflux_driver_ipc/src/driver.rs`
- `docs/RPC_SERVICES_CONFIG.md`
