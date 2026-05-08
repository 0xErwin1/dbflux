---
name: dbflux-rpc-driver
description: >
  Create external DBFlux database drivers that speak the Driver RPC protocol.
  Trigger: When adding an RPC driver, external database service, driver plugin, or service configured through Settings → RPC Services.
license: MIT
---

## When to Use

- The driver should live outside the DBFlux binary.
- The user mentions RPC services, external drivers, plugins, sockets, or `dbflux-driver-host`.
- The integration should be configured through Settings → RPC Services.

## Source of Truth

- `docs/DRIVER_RPC_PROTOCOL.md`
- `crates/dbflux_ipc/src/driver_protocol.rs`
- `crates/dbflux_driver_ipc/src/driver.rs`
- `crates/dbflux_driver_ipc/src/transport.rs`
- `crates/dbflux_driver_host/src/main.rs`
- `crates/dbflux_app/src/rpc_services.rs`
- `examples/custom_driver/`

## Critical Patterns

- Start from `examples/custom_driver/` unless the user already has service code.
- DBFlux stores launch config in `cfg_services`, `cfg_service_args`, and `cfg_service_env` and probes services on startup.
- Internal registry key is `rpc:<socket_id>`.
- The service is the source of truth for driver kind, metadata, and form definition.
- Restart DBFlux after changing persisted RPC service settings.

## Protocol Checklist

1. Bind the local socket using DBFlux IPC conventions.
2. Handle `Hello` and negotiate the highest compatible protocol version.
3. Return `driver_kind`, `driver_metadata`, and `form_definition` from `Hello`.
4. Handle `OpenSession`, `CloseSession`, and `Ping`.
5. Parse `DbConfig::External { kind, values }` from `OpenSession` profile JSON.
6. Validate required form values server-side; use `InvalidRequest` for malformed profiles.
7. Implement at least one useful requested operation such as `Execute`, `Schema`, or browse/CRUD methods.
8. Return `UnsupportedMethod` for intentionally unsupported requests.
9. Keep `Hello` metadata and `SessionOpened` metadata consistent.
10. Stamp every post-`Hello` envelope with the negotiated version.

## Launch Facts

- Empty `command` and empty `args`: DBFlux expects the service to already be running.
- Empty `command` with non-empty `args`: DBFlux launches `dbflux-driver-host` for driver services.
- Explicit `command`: DBFlux starts and owns that process, then kills it on shutdown.
- Use absolute command paths while testing to avoid PATH ambiguity.

## Commands

```bash
cargo build
RUST_LOG=info cargo run -- --socket <socket-id>
cargo fmt --all -- --check
```
