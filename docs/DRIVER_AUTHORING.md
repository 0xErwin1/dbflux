# Driver Authoring Guide

Use this guide to choose and implement a DBFlux database-driver integration. It covers the contributor path without repeating the broader architecture or RPC protocol references.

## Choose an integration path

| Choose | Built-in Rust driver | External RPC driver |
| --- | --- | --- |
| Best fit | The driver should ship in the DBFlux workspace and process | The driver should run out of process or be developed and deployed independently |
| Implementation | A `crates/dbflux_driver_<name>/` crate implementing core Rust contracts | A service implementing the driver RPC protocol |
| Registration | Compile-time feature wiring and `AppState::build_builtin_drivers()` | Settings -> RPC Services with `kind=driver` and a `socket_id` |
| Stable key | `builtin:<name>` | `rpc:<socket_id>` |
| Configuration | Driver-owned `DriverFormDef` converted to a built-in `DbConfig` variant | Form data supplied by the handshake and stored as `DbConfig::External` |

## Built-in driver: happy path

1. Copy the structure of the closest existing `crates/dbflux_driver_*/` crate.
2. Implement `DbDriver` and `Connection`, including metadata, form/config conversion, connection behavior, errors, and typed result columns.
3. Declare only capabilities backed by working implementations; add optional seams only when the driver supports them.
4. Wire the crate and feature through the workspace, app, and binary, then register it in `build_builtin_drivers()`.
5. Add focused tests, a crate README, and update the driver support matrix.

The [detailed built-in checklist](#built-in-driver-checklist) expands each step.

## External RPC driver: happy path

1. Implement the canonical [Driver RPC Protocol](DRIVER_RPC_PROTOCOL.md), using the [custom driver example](../examples/custom_driver/README.md) as a starting point.
2. Build and run the service, either independently or with a managed command.
3. Add it under Settings -> RPC Services with `kind=driver`, a stable `socket_id`, and the optional managed command.
4. Restart DBFlux and verify that the handshake-provided metadata and form appear in the connection manager.

See the [RPC Services configuration reference](RPC_SERVICES_CONFIG.md) for current configuration behavior. Do not copy launch flags from this guide; the protocol, configuration reference, and example are authoritative.

## Core contracts and decoupling rule

The primary contract is [`DbDriver` plus `Connection`](../crates/dbflux_core/src/core/traits.rs):

- `DbDriver` supplies driver metadata, its connection form definition, config construction and extraction, connection construction, and a stable `DriverKey`.
- `Connection` supplies runtime query, schema, mutation, and optional capability-specific behavior. Defaults exist for many unsupported operations; required methods and advertised capabilities must still agree.
- Built-in `driver_key()` values use `builtin:<name>`. External drivers use `rpc:<socket_id>`.

Metadata and adaptation are defined by [`DriverMetadata`, `DatabaseCategory`, `QueryLanguage`, and `DriverCapabilities`](../crates/dbflux_core/src/driver/capabilities.rs), including generic editor-presentation metadata. Runtime source and presentation behavior is exposed through generic seams on `Connection` in [`traits.rs`](../crates/dbflux_core/src/core/traits.rs).

**Strict rule:** UI and app workflow code must not branch on concrete driver IDs. Adapt from metadata, category, query language, capability flags, form definitions, and generic source/presentation seams. If a new UI distinction is necessary, add a generic core contract that another driver could implement.

For result data, populate [`ColumnMeta.kind` with `ColumnKind`](../crates/dbflux_core/src/query/types.rs). Charts and other consumers use this semantic kind; they do not infer it from a driver ID or `type_name`.

## Built-in driver checklist

### 1. Crate and contracts

- [ ] Add `crates/dbflux_driver_<name>/Cargo.toml`, `src/lib.rs`, implementation modules, and tests. Follow the closest driver rather than assuming every driver has identical modules.
- [ ] Implement `DbDriver` and a thread-safe `Connection` from [`crates/dbflux_core/src/core/traits.rs`](../crates/dbflux_core/src/core/traits.rs).
- [ ] Return a stable `DriverKey` in the form `builtin:<name>`.
- [ ] Keep database-client types and driver-specific behavior inside the driver crate; expose behavior through core contracts.

### 2. Metadata and capabilities

- [ ] Define factual `DriverMetadata`: identity, display fields, `DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`, connection defaults, and applicable generic capability structures.
- [ ] Use metadata and generic presentation/source seams for UI adaptation. Do not add driver-ID conditionals to UI or app workflows.
- [ ] Advertise a capability only when the corresponding operation or optional seam works. Confirm negative claims as well as supported behavior.

### 3. Forms and configuration

- [ ] Define and own the crate's `DriverFormDef`; the connection UI renders it generically.
- [ ] Implement `build_config()` validation and `extract_values()` editing round trips.
- [ ] Keep secrets on the established secret paths rather than embedding them in persisted form values.
- [ ] Implement URI parsing/building or export-field overrides only when applicable.

### 4. Connections, errors, and results

- [ ] Construct and test the connection through the `DbDriver` methods, including required secret handling and connection tests.
- [ ] Implement structured query and connection error formatting through [`QueryErrorFormatter` and `ConnectionErrorFormatter`](../crates/dbflux_core/src/core/error_formatter.rs). Preserve useful database context without exposing secrets.
- [ ] Return schema and query data through core types, including `ColumnMeta.kind` for every result column using the correct `ColumnKind`.
- [ ] Test type mapping directly. Do not rely on consumers to derive semantics from raw `type_name` strings.

### 5. Optional seams

Implement these only when the database supports them, and keep capability flags synchronized with the implementation:

- [ ] A non-default `LanguageService` for language-specific validation and mutation classification.
- [ ] SQL dialect, code generator, query generator, or semantic planner behavior as applicable.
- [ ] Source context, metric catalog, dashboard importer, or dashboard source behavior as applicable.
- [ ] An instance catalog for metrics or inspectors as applicable.
- [ ] Other schema, CRUD, cancellation, transfer, or key-value seams represented by the core traits and capability flags.

### 6. Feature wiring and registration

- [ ] Add workspace membership and a workspace dependency in the root [`Cargo.toml`](../Cargo.toml).
- [ ] Add an optional dependency and feature relay in [`crates/dbflux_app/Cargo.toml`](../crates/dbflux_app/Cargo.toml).
- [ ] Forward the binary feature in [`crates/dbflux/Cargo.toml`](../crates/dbflux/Cargo.toml).
- [ ] Add feature-gated imports and registration in [`AppState::build_builtin_drivers()`](../crates/dbflux_app/src/app_state/bootstrap.rs).
- [ ] Verify both the enabled feature and a representative feature-disabled build so registration remains properly gated.

### 7. Tests and documentation

- [ ] Test metadata, capability declarations, form/config round trips, errors, connection behavior, schema mapping, query results, and every advertised optional seam.
- [ ] Add integration tests where behavior crosses the driver/core boundary; keep live service tests ignored or gated according to existing crate conventions.
- [ ] Add `crates/dbflux_driver_<name>/README.md` with clear **Features** and **Limitations** sections.
- [ ] Update [`docs/DRIVERS.md`](DRIVERS.md) and keep its capability claims aligned with the crate README and implementation.

## External RPC driver checklist

- [ ] Implement handshake, form, session, query, and supported optional operations against the [Driver RPC Protocol](DRIVER_RPC_PROTOCOL.md).
- [ ] Supply metadata, capabilities, and the form definition through the protocol handshake; keep every capability claim aligned with implemented RPC operations.
- [ ] Configure the service under Settings -> RPC Services as `kind=driver` with a stable `socket_id`; add a managed command only when DBFlux should own the process lifecycle.
- [ ] Expect the runtime key `rpc:<socket_id>` and generic configuration storage through `DbConfig::External`.
- [ ] Follow [RPC Services Config](RPC_SERVICES_CONFIG.md) for persisted configuration and lifecycle semantics.
- [ ] Build and smoke-test from the [custom driver example](../examples/custom_driver/README.md), then test restart, handshake failure, unsupported operations, and connection-form round trips.

External RPC drivers do not use the built-in Cargo feature wiring or `build_builtin_drivers()` registration path.

## Review checklist

Before opening a PR, confirm:

- [ ] The selected built-in or RPC path is used consistently; the two registration paths are not mixed.
- [ ] No UI or app workflow branches on a concrete driver ID.
- [ ] Metadata, capability flags, optional seams, tests, crate README, and `docs/DRIVERS.md` make the same claims.
- [ ] Form/config editing round trips work and secrets are not persisted or logged unexpectedly.
- [ ] Query results populate `ColumnMeta.kind` correctly.
- [ ] Connection and query failures produce structured, useful, secret-safe errors.
- [ ] Built-in feature-disabled and feature-enabled builds pass, or the RPC service completes its handshake and restart smoke test.
- [ ] The repository checks in [CONTRIBUTING.md](../CONTRIBUTING.md) pass.
