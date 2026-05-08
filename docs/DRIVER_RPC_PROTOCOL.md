# Driver RPC Protocol Specification

This document defines how DBFlux discovers, launches, and talks to RPC services over local IPC.

DBFlux now activates two runtime service families:

- `RpcServiceKind::Driver` -> runtime database drivers
- `RpcServiceKind::AuthProvider` -> runtime auth-provider registries in the app and MCP server

## Source of truth

For active driver services, **the service is the source of truth** for:

- driver kind (`DbKind`)
- driver metadata (`DriverMetadataDto`: name, icon, category, capabilities, query language, etc.)
- connection form definition (`DriverFormDefDto`)

DBFlux stores launch configuration in its SQLite-backed services config. RPC services are created and edited from **Settings → RPC Services**.

## Integration model

At app startup, DBFlux loads configured RPC services from `~/.local/share/dbflux/dbflux.db`, then for each service:

1. discovers the persisted service descriptor, including `RpcServiceKind`
2. branches by `kind`
3. ensures the service is running (starts it if needed)
4. performs the family-specific `Hello` handshake
5. reads runtime metadata from the service
6. registers the adapted runtime service in the appropriate in-memory registry

If any step fails, that service is skipped without aborting startup. Driver failures do not break auth providers, and auth-provider failures do not break drivers.

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

Notes:

- `socket_id` is required.
- `kind` supports `driver` and `auth_provider`.
- `kind` supports `driver` and `auth_provider`.
- `command` is optional.
  - If `command` is omitted and `args` is empty, DBFlux expects the service to already be running.
  - For `driver`, if `command` is omitted and `args` is non-empty, DBFlux launches `dbflux-driver-host`.
  - For `auth_provider`, managed launch requires an explicit `command`; DBFlux does not assume a default host binary.
- `args`, `env`, and `startup_timeout_ms` are optional.
- DBFlux derives an internal driver registry key as `rpc:<socket_id>`.
- Only `driver` services are registered as database drivers.
- `auth_provider` services are registered only in auth-provider registries and never receive a `rpc:<socket_id>` driver identity.

## Handshake contract

DBFlux connects and sends `Hello` first.

The active driver RPC API family is `driver_rpc`. In the current dedicated driver RPC transport, that family is implicit in the protocol itself rather than transmitted on the wire during `Hello`. Compatibility is enforced by the driver RPC endpoint plus the selected protocol major version; minor versions are additive and are negotiated deterministically within that major line.

Client request:

```rust
DriverRequestBody::Hello(DriverHelloRequest {
    client_name: "dbflux_driver_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![ProtocolVersion::new(1, 0), ProtocolVersion::new(1, 1)],
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

If multiple compatible minors overlap, the host must select the highest mutual minor version.

If no compatible version exists, return `DriverRpcErrorCode::VersionMismatch`.

After `Hello`, every request and response envelope must use the negotiated `selected_version`. A peer that receives a different post-handshake envelope version must reject it as a version mismatch.

Current validation boundary:

- DBFlux persists per-service API family/version metadata for discovery and future runtime seams.
- The live driver handshake currently validates negotiated protocol versions, but it does not transmit or separately re-validate the API family string on the wire because the driver RPC transport is already family-specific.

## Auth-provider RPC contract

The active auth-provider RPC API family is `auth_provider_rpc` at `1.2`.

DBFlux uses persisted `api_family` / `api_major` metadata as a startup preflight. Compatible rows then negotiate the highest shared minor version during `Hello`.

Client request:

```rust
AuthProviderRequestBody::Hello(AuthProviderHelloRequest {
    client_name: "dbflux_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![
        ProtocolVersion::new(1, 2),
        ProtocolVersion::new(1, 1),
        ProtocolVersion::new(1, 0),
    ],
    auth_token: Some("<token>".to_string()),
})
```

Server response must include:

- `selected_version`
- `provider_id`
- `display_name`
- `form_definition`

The v1.2 `Hello` response additionally carries `secret_dependency_opt_in` (`bool`), declaring whether the provider opts in to receiving secret field values inside dependency maps for dynamic option lookups. When `false` (default), DBFlux strips secret values from dependency maps before forwarding `FetchDynamicOptions` requests.

Supported request / response flow:

| Request | Response | Purpose |
|---|---|---|
| `Hello` | `Hello` | protocol negotiation + provider identity |
| `ValidateSession` | `SessionState` | validate cached auth state |
| `Login` | `LoginUrlProgress?` + `LoginResult` | optional verification URL + terminal login result |
| `ResolveCredentials` | `Credentials` | resolve runtime credential fields |
| `FetchDynamicOptions` | `DynamicOptions` | resolve dynamic dropdown options for a `DynamicSelect` form field (v1.2+) |

Notes:

- `Login` may emit zero or one `LoginUrlProgress` event before `LoginResult`.
- If no progress event is sent, DBFlux treats the verification URL callback as `None`.
- `FetchDynamicOptions` is available only when the negotiated version is at least `1.2`. Providers that negotiate below v1.2 receive a permanent "not supported" outcome from the host without an IPC round-trip.
- `detect_importable_profiles`, profile write-back hooks, and provider-specific value-provider registration are intentionally out of scope for the RPC contract in this change.
- Auth-provider runtime failures surface through existing `DbError` handling and do not abort startup.

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

Use `InvalidRequest` for malformed profiles/form values and `UnsupportedMethod` for methods intentionally not implemented. Auth-provider RPC uses the parallel `AuthProviderRpcErrorCode` set with the same operational meaning (`VersionMismatch`, `UnsupportedMethod`, `Timeout`, `Transport`, etc.).

## Process lifecycle and cleanup

When DBFlux starts a service process itself (via `command` or the supported default host command), that process is tracked as a managed host.

On DBFlux shutdown:

- all tracked managed hosts are killed (`kill + wait`)
- hosts started manually outside DBFlux are not tracked and are not killed

This guarantees DBFlux cleans up only the processes it owns.

If a managed host exits early or times out before the socket is ready, DBFlux reports the service id together with a bounded tail of recent stdout/stderr to aid troubleshooting.

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
10. stamp every post-`Hello` envelope with the negotiated version instead of assuming the latest constant

## Working example in this repository

Use:

- `examples/custom_driver/src/main.rs`
- `examples/custom_driver/README.md`
- `examples/custom_auth_provider/src/main.rs`
- `examples/custom_auth_provider/README.md`

Those examples are compatible with the current active driver-service integration model.

Quick test path:

1. add a new **Driver** service in **Settings → RPC Services**
2. point `command` to your built example binary
3. set `args` to `--socket <your-socket-id>`
4. restart DBFlux
5. create either a connection (driver example) or an auth profile (auth-provider example) through the UI forms exposed by the service

## References

- `crates/dbflux_ipc/src/driver_protocol.rs`
- `crates/dbflux_driver_ipc/src/transport.rs`
- `crates/dbflux_driver_host/src/main.rs`
- `crates/dbflux/src/app.rs`
- `crates/dbflux_driver_ipc/src/driver.rs`
- `docs/RPC_SERVICES_CONFIG.md`
