# RPC Services Config Reference

This file documents the storage and management of RPC services in DBFlux.

DBFlux now persists a first-class RPC services foundation through `RpcServiceKind`:

- `Driver` — adapted into runtime database drivers
- `AuthProvider` — adapted into runtime auth-provider registries in both the app and the MCP server

## Storage

RPC services are stored in SQLite at `~/.local/share/dbflux/dbflux.db`, not in a JSON file.

**Tables:**

- `cfg_services` — main service record (socket_id, service_kind, command, startup_timeout_ms, enabled)
- `cfg_services.api_family`, `cfg_services.api_major`, `cfg_services.api_minor` — optional RPC API contract metadata
- `cfg_service_args` — ordered process arguments
- `cfg_service_env` — environment variables

## Schema

```sql
CREATE TABLE cfg_services (
    socket_id TEXT NOT NULL UNIQUE,
    service_kind TEXT NOT NULL DEFAULT 'driver',
    command TEXT,
    startup_timeout_ms INTEGER DEFAULT 5000,
    enabled INTEGER DEFAULT 1,
    api_family TEXT,
    api_major INTEGER,
    api_minor INTEGER
);

CREATE TABLE cfg_service_args (
    id INTEGER PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(socket_id),
    position INTEGER NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE cfg_service_env (
    id INTEGER PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(socket_id),
    key TEXT NOT NULL,
    value TEXT NOT NULL
);
```

## Managing Services

Services are managed through the Settings UI under the **RPC Services** section, not by editing files directly.

To add or edit a service:
1. Open Settings → RPC Services
2. Add a new service or select an existing one
3. Choose the service kind (`Driver` or `Auth Provider`)
4. Configure socket ID, command path, arguments, environment variables, and timeout
5. Save changes

Notes:

- `Driver` services are active in the runtime and keep the existing `rpc:<socket_id>` driver identity.
- `Auth Provider` services are active in runtime auth-provider registries only; they never appear as drivers.
- DBFlux preserves compatibility for driver registration IDs as `rpc:<socket_id>`.
- If API metadata is missing on an existing driver row, DBFlux defaults it to the current `driver_rpc` contract at version `1.1`.
- If API metadata is missing on an auth-provider row, DBFlux defaults it to the current `auth_provider_rpc` contract at version `1.0`.
- `api_family` / `api_major` are used as startup preflight for auth providers before DBFlux probes the socket.

## Legacy Migration

On first startup after upgrading, DBFlux imports any existing RPC services from `~/.config/dbflux/config.json` into `cfg_services`. This is handled automatically by `dbflux_storage/src/legacy.rs` and is idempotent (tracked in `sys_legacy_imports`).

The legacy `config.json` format had this structure:

```json
{
  "services": [
    {
      "socket_id": "my-driver.sock",
      "command": "/absolute/path/to/driver",
      "args": ["--socket", "my-driver.sock"],
      "env": {
        "RUST_LOG": "info"
      },
      "startup_timeout_ms": 5000
    }
  ]
}
```

This is converted to the SQLite schema automatically. Legacy rows are treated as `service_kind='driver'`. The `config.json` file itself is not used after import.

## Semantics

- `socket_id` is used literally as the socket filename
- DBFlux internally identifies each service as `rpc:<socket_id>`
- DBFlux classifies each service by `service_kind` before runtime adaptation
- Driver name/icon/category/form come from the service's `Hello` response (`driver_metadata`, `form_definition`), not from configuration
- Services with `service_kind='driver'` that fail to complete the RPC handshake (`Hello`) during startup are not registered
- Services with `service_kind='auth_provider'` are loaded into auth-provider registries when they pass compatibility checks and probe successfully
- Driver-path negotiation selects the highest mutually supported compatible minor version during `Hello`, then requires every later envelope to use that exact negotiated version
- Auth-provider negotiation follows the same family/major/minor scheme under `auth_provider_rpc`; incompatible family or major versions are skipped before registration

## Fields

- `socket_id` (required): local socket name used by DBFlux and the service.
  - Allowed characters: ASCII letters, numbers, `.`, `_`, `-`
  - Path separators, spaces, and other punctuation are rejected.
  - The value is passed to the platform socket namespace as-is, so keep it short and stable.
- `command` (optional): executable to run when DBFlux needs to start the service.
  - If omitted and `args` is also empty, DBFlux treats the service as already running and does not spawn anything.
  - For `driver`, if omitted and `args` is non-empty, DBFlux launches `dbflux-driver-host`.
  - For `auth_provider`, if DBFlux must launch the service, `command` must be set explicitly.
- `args` (optional): process arguments.
- `env` (optional): environment variables for the spawned process.
- `startup_timeout_ms` (optional): max wait time for socket readiness after spawn.
  - Default: `5000`

## Common Mistakes

- Mismatched socket names between the service configuration and service args
- Relative `command` path that does not resolve under the DBFlux process environment
- Editing the database directly instead of through the Settings UI
- Service not implementing required `Hello` fields for the current RPC protocol version
- Omitting `command` while providing partial `args`; if you want DBFlux to launch the default host, `args` must include both `--driver` and `--socket`.
- Configuring an auth-provider service with `args` but no `command`; DBFlux will reject that launch config instead of assuming the driver host
