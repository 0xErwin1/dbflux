# Referencia de la UI de RPC Services

Este archivo documenta el almacenamiento y la gestión de los RPC services en
DBFlux.

DBFlux ahora persiste una base de RPC services de primera clase a través de
`RpcServiceKind`:

- `Driver` — se adapta a drivers de base de datos en runtime
- `AuthProvider` — se adapta a registros de auth provider en runtime, tanto en
  la app como en el MCP server

## Storage

Los RPC services se almacenan en SQLite en `~/.local/share/dbflux/dbflux.db`, no
en un archivo JSON.

**Tablas:**

- `cfg_services` — registro principal del service (socket_id, service_kind,
  command, startup_timeout_ms, enabled)
- `cfg_services.api_family`, `cfg_services.api_major`, `cfg_services.api_minor`
  — metadata opcional del contrato de la API RPC
- `cfg_service_args` — argumentos de proceso ordenados
- `cfg_service_env` — variables de entorno

## Schema

```sql
-- Base table (migration 001). `service_kind` is added by migration 005 and
-- `api_family`/`api_major`/`api_minor` by migration 006; they are shown here
-- inline for reference but are not part of the base DDL.
CREATE TABLE cfg_services (
    socket_id TEXT PRIMARY KEY,
    enabled INTEGER DEFAULT 1,
    command TEXT,
    startup_timeout_ms INTEGER,        -- no SQL-level default; the 5000ms
                                       -- fallback (DEFAULT_STARTUP_TIMEOUT_MS)
                                       -- is applied in app code
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    service_kind TEXT NOT NULL DEFAULT 'driver',  -- added by migration 005
    api_family TEXT,                              -- added by migration 006
    api_major INTEGER,                            -- added by migration 006
    api_minor INTEGER                             -- added by migration 006
);

CREATE TABLE cfg_service_args (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(socket_id),
    position INTEGER NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE cfg_service_env (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(socket_id),
    key TEXT NOT NULL,
    value TEXT NOT NULL
);
```

## Gestión de Services

Los services se gestionan a través de la UI de Settings, en la sección **RPC
Services**, no editando archivos directamente.

Para agregar o editar un service:
1. Abre Settings → RPC Services
2. Agrega un nuevo service o selecciona uno existente
3. Elige el service kind (`Driver` o `Auth Provider`)
4. Configura el socket ID, la ruta del command, arguments, environment
   variables, y el timeout
5. Guarda los cambios

Notas:

- Los services `Driver` están activos en el runtime y conservan la identidad de
  driver existente `rpc:<socket_id>`.
- Los services `Auth Provider` están activos únicamente en los registros de auth
  provider en runtime; nunca aparecen como drivers.
- DBFlux preserva la compatibilidad de los IDs de registro de driver como
  `rpc:<socket_id>`.
- Si falta la metadata de API en una fila de driver existente, DBFlux la define
  por defecto según el contrato `driver_rpc` actual en la versión `1.1`.
- Si falta la metadata de API en una fila de auth-provider, DBFlux la define por
  defecto según el contrato `auth_provider_rpc` actual en la versión `1.2`.
- `api_family` / `api_major` se usan como preflight de arranque para los auth
  providers antes de que DBFlux sondee el socket.

## Semántica

- `socket_id` se usa literalmente como el nombre del archivo del socket
- DBFlux identifica internamente cada service como `rpc:<socket_id>`
- DBFlux clasifica cada service por `service_kind` antes de la adaptación en
  runtime
- El nombre/icon/category/form del driver vienen de la respuesta `Hello` del
  service (`driver_metadata`, `form_definition`), no de la configuración
- Los services con `service_kind='driver'` que fallan al completar el handshake
  RPC (`Hello`) durante el arranque no se registran
- Los services con `service_kind='auth_provider'` se cargan en los registros de
  auth provider cuando pasan los chequeos de compatibilidad y sondean
  exitosamente
- La negociación del driver-path selecciona la minor version compatible
  mutuamente soportada más alta durante `Hello`, y luego requiere que cada
  envelope posterior use exactamente esa versión negociada
- La negociación de auth-provider sigue el mismo esquema family/major/minor bajo
  `auth_provider_rpc`; las family o major versions incompatibles se omiten antes
  del registro

## Campos

- `socket_id` (requerido): nombre de socket local usado por DBFlux y el service.
  - Caracteres permitidos: letras ASCII, números, `.`, `_`, `-`
  - Los separadores de ruta, espacios, y otra puntuación se rechazan.
  - El valor se pasa tal cual al namespace de socket de la plataforma, así que
    mantenlo corto y estable.
- `command` (opcional): ejecutable a correr cuando DBFlux necesita arrancar el
  service.
  - Si se omite y `args` también está vacío, DBFlux trata el service como ya
    corriendo y no lanza nada.
  - Para `driver`, si se omite y `args` no está vacío, DBFlux lanza
    `dbflux-driver-host`.
  - Para `auth_provider`, si DBFlux debe lanzar el service, `command` debe
    fijarse explícitamente.
- `args` (opcional): argumentos del proceso.
- `env` (opcional): variables de entorno para el proceso lanzado.
- `startup_timeout_ms` (opcional): tiempo máximo de espera para que el socket
  esté listo después del spawn.
  - Default: `5000`

## Errores Comunes

- Nombres de socket desalineados entre la configuración del service y los args
  del service
- Ruta relativa de `command` que no se resuelve bajo el entorno del proceso de
  DBFlux
- Editar la base de datos directamente en lugar de a través de la UI de Settings
- El service no implementa los campos requeridos de `Hello` para la versión
  actual del protocolo RPC
- Omitir `command` mientras se proveen `args` parciales; si quieres que DBFlux
  lance el host por defecto, `args` debe incluir tanto `--driver` como
  `--socket`.
- Configurar un service de auth-provider con `args` pero sin `command`; DBFlux
  rechazará ese launch config en lugar de asumir el driver host
