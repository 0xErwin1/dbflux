# RPC Services Config Reference

This file documents the storage and management of external RPC driver services in DBFlux.

## Storage

RPC services are stored in SQLite at `~/.local/share/dbflux/dbflux.db`, not in a JSON file.

**Tables:**

- `cfg_services` — main service record (socket_id, command, startup_timeout_ms, enabled)
- `cfg_service_args` — ordered process arguments
- `cfg_service_env` — environment variables

## Schema

```sql
CREATE TABLE cfg_services (
    id TEXT PRIMARY KEY,
    socket_id TEXT NOT NULL UNIQUE,
    command TEXT,
    startup_timeout_ms INTEGER DEFAULT 5000,
    enabled INTEGER DEFAULT 1
);

CREATE TABLE cfg_service_args (
    id INTEGER PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(id),
    position INTEGER NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE cfg_service_env (
    id INTEGER PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(id),
    key TEXT NOT NULL,
    value TEXT NOT NULL
);
```

## Managing Services

Services are managed through the Settings UI under the **Services** section, not by editing files directly.

To add or edit a service:
1. Open Settings → Services
2. Add a new service or select an existing one
3. Configure socket ID, command path, arguments, environment variables, and timeout
4. Save changes

## Legacy Migration

On first startup after upgrading, DBFlux imports any existing RPC services from `~/.config/dbflux/config.json` into `cfg_services`. This is handled automatically by `dbflux_storage/src/legacy.rs` and is idempotent (tracked in `sys_legacy_imports`).

The legacy `config.json` format had this structure:

```json
{
  "rpc_services": [
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

This is converted to the SQLite schema automatically. The `config.json` file itself is not used after import.

## Semantics

- `socket_id` is used literally as the socket filename
- DBFlux internally identifies each service as `rpc:<socket_id>`
- Driver name/icon/category/form come from the service's `Hello` response (`driver_metadata`, `form_definition`), not from configuration
- Services that fail to complete the RPC handshake (`Hello`) during startup are not registered

## Common Mistakes

- Mismatched socket names between the service configuration and service args
- Relative `command` path that does not resolve under the DBFlux process environment
- Editing the database directly instead of through the Settings UI
- Service not implementing required `Hello` fields for the current RPC protocol version
