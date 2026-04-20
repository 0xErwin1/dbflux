# Driver RPC Protocol Specification

This document defines how DBFlux discovers, launches, and talks to RPC services over local IPC.

Today, only RPC services with `RpcServiceKind::Driver` are adapted into runtime database drivers. `RpcServiceKind::AuthProvider` is already persisted and discoverable, but it is not wired into runtime auth features yet.

## Source of truth

For active driver services, **the service is the source of truth** for:

- driver kind (`DbKind`)
- driver metadata (`DriverMetadataDto`: name, icon, category, capabilities, query language, etc.)
- connection form definition (`DriverFormDefDto`)

DBFlux stores launch configuration in its SQLite-backed services config. Legacy `config.json` rows are imported for compatibility, but the runtime no longer reads that file on normal startup.

## Integration model

At app startup, DBFlux loads configured RPC services from `~/.local/share/dbflux/dbflux.db`, then for each service:

1. discovers the persisted service descriptor, including `RpcServiceKind`
2. adapts only `Driver` services into the current driver bootstrap path
3. ensures the service is running (starts it if needed)
4. performs a `Hello` handshake
5. reads `driver_kind`, `driver_metadata`, and `form_definition` from the service
6. registers the driver in-memory so it appears in the connection manager

If driver adaptation or handshake fails, that service is skipped and not shown in the UI as a driver. Non-driver service kinds remain persisted but inert.

Important behavior:

- Service configuration is read at startup. Restart DBFlux after changing RPC service settings.
- `socket_id` is used as-is (it is not rewritten by DBFlux).
- Internal registry key is `rpc:<socket_id>`.

## Transport

DBFlux uses local sockets via `interprocess`:

- **Linux**: abstract namespace Unix sockets (`\0name`)
- **macOS**: Unix sockets in `/tmp/`
- **Windows**: named pipes (`\\.\pipe\...`)

Messages are framed as:

- 4-byte little-endian length (`u32`)
- bincode payload

Maximum message size: `16 MiB`.

Socket cleanup is automatic on process exit/drop (provided by `interprocess`).

## Runtime configuration

Primary storage: `~/.local/share/dbflux/dbflux.db` (`cfg_services`, `cfg_service_args`, `cfg_service_env`)

Settings UI: **Settings → RPC Services**

Schema used by DBFlux:

```json
{
  "rpc_services": [
    {
      "socket_id": "my-driver.sock",
      "kind": "driver",
      "command": "/absolute/path/to/driver-binary",
      "args": ["--socket", "my-driver.sock"],
      "env": {
        "RUST_LOG": "info"
      },
      "startup_timeout_ms": 5000
    }
  ]
}
```

Notes:

- `socket_id` is required.
- `kind` supports `driver` and `auth_provider`.
- `command` is optional. If omitted, DBFlux uses `dbflux-driver-host`.
- `args`, `env`, and `startup_timeout_ms` are optional.
- DBFlux derives an internal driver registry key as `rpc:<socket_id>`.
- Only `driver` services are registered as database drivers today.
- `auth_provider` services are stored and discovered but not yet consumed by runtime auth flows.

## Handshake contract

DBFlux connects and sends `Hello` first.

Client request:

```rust
DriverRequestBody::Hello(DriverHelloRequest {
    client_name: "dbflux_driver_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![DRIVER_RPC_VERSION],
    requested_capabilities: vec![
        DriverCapability::Cancellation,
        DriverCapability::ChunkedResults,
        DriverCapability::SchemaIntrospection,
        DriverCapability::MultiDatabase,
    ],
})
```

Server response must include:

- `selected_version`
- `capabilities`
- `driver_kind`
- `driver_metadata`
- `form_definition`

Example:

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

If no compatible version exists, return `DriverRpcErrorCode::VersionMismatch`.

## Form contract

The connection form shown in DBFlux is built from `form_definition` returned in `Hello`.

- The service defines fields/tabs/sections.
- DBFlux validates required fields in UI.
- On connect/save, DBFlux sends collected values through `DbConfig::External.values` in `OpenSession` profile JSON.

If `form_definition.tabs` is empty, the connection form will show no driver-specific inputs.

## Session lifecycle

1. `Hello`
2. `OpenSession`
3. request/response operations
4. `CloseSession`

`OpenSession` still returns `SessionOpened` with metadata. Keep this consistent with `Hello` metadata.

DBFlux sends the saved profile JSON to `OpenSession`. For external drivers, the profile config is:

```rust
DbConfig::External {
    kind: DbKind,
    values: HashMap<String, String>,
}
```

`values` contains the field values collected from your `form_definition`.

The service should parse `profile_json`, expect `DbConfig::External`, and validate required fields again server-side.

## Request/response overview

| Request | Response | Purpose |
|---|---|---|
| `Hello` | `Hello` | protocol negotiation + driver identity |
| `OpenSession` | `SessionOpened` | open connection/session |
| `CloseSession` | `SessionClosed` | close session |
| `Ping` | `Pong` | liveness |
| `Execute` | `ExecuteResult` | query execution |
| `Schema` | `Schema` | schema snapshot |
| `ListDatabases` | `Databases` | database list |

The protocol also supports browse, CRUD, key-value, and code generation operations. See `crates/dbflux_ipc/src/driver_protocol.rs` for the full enum set.

## Error handling

Return structured errors through `DriverResponseBody::Error(DriverRpcError { ... })`.

Common codes:

- `InvalidRequest`
- `UnsupportedMethod`
- `VersionMismatch`
- `SessionNotFound`
- `Timeout`
- `Cancelled`
- `Transport`
- `Driver`
- `Internal`

Use `InvalidRequest` for malformed profiles/form values and `UnsupportedMethod` for methods intentionally not implemented.

## Process lifecycle and cleanup

When DBFlux starts a service process itself (via `command`), that process is tracked as a managed host.

On DBFlux shutdown:

- all tracked managed hosts are killed (`kill + wait`)
- hosts started manually outside DBFlux are not tracked and are not killed

This guarantees DBFlux cleans up only the processes it owns.

## Minimal implementation checklist

Your service should:

1. bind socket via `interprocess`
2. handle `Hello` and return metadata/kind
3. return a form definition in `Hello`
4. handle `OpenSession`/`CloseSession`
5. implement at least one useful operation (`Execute`)
6. return `UnsupportedMethod` for non-implemented operations

Recommended:

7. validate `DbConfig::External.values` in `OpenSession`
8. return clear `InvalidRequest` errors for missing/invalid form values
9. keep `Hello` metadata and `SessionOpened` metadata consistent

## Working example in this repository

Use:

- `examples/custom_driver/src/main.rs`
- `examples/custom_driver/config.example.json`

That example is compatible with the current active driver-service integration model.

Quick test path:

1. add a new **Driver** service in **Settings → RPC Services**
2. copy the values from `examples/custom_driver/config.example.json`
3. update `command` to your absolute binary path
4. restart DBFlux
5. create a connection using the external driver form fields

## References

- `crates/dbflux_ipc/src/driver_protocol.rs`
- `crates/dbflux_driver_ipc/src/transport.rs`
- `crates/dbflux_driver_host/src/main.rs`
- `crates/dbflux/src/app.rs`
- `crates/dbflux_driver_ipc/src/driver.rs`
- `docs/RPC_SERVICES_CONFIG.md`
