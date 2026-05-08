---
name: dbflux-builtin-driver
description: >
  Add native built-in DBFlux database drivers using the repository's driver traits and registration flow.
  Trigger: When adding a built-in DBFlux driver, native database adapter, or `crates/dbflux_driver_*` crate.
license: MIT
---

## When to Use

- The user wants database support compiled into DBFlux.
- The change creates or extends `crates/dbflux_driver_<name>/`.
- The driver should appear without configuring an RPC service.

## Source of Truth

- `ARCHITECTURE.md`
- `crates/dbflux_core/src/core/traits.rs`
- `crates/dbflux_core/src/driver/capabilities.rs`
- `crates/dbflux_core/src/driver/form.rs`
- `crates/dbflux_app/src/app_state.rs`
- Existing `crates/dbflux_driver_*/` crates and their `README.md` files

## Critical Patterns

- Implement `DbDriver` and `Connection`; do not add driver-specific UI branches.
- Declare UI behavior through `DriverMetadata`, `DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`, `DriverFormDef`, and optional generic seams.
- Use a stable built-in driver key consistent with existing drivers.
- Implement structured error formatting through `ErrorFormatter` when backend errors expose detail/hint/code fields.
- Implement `QueryGenerator` when DBFlux must generate native read/mutation templates or MCP previews.
- Keep schema loading shallow by default; fetch expensive details on demand.

## Checklist

1. Create or update `crates/dbflux_driver_<name>/`.
2. Add workspace and crate manifest entries following existing driver crates.
3. Add the feature flag in the `dbflux` crate manifest.
4. Register the driver in `AppState::build_builtin_drivers()` behind that feature flag.
5. Add `crates/dbflux_driver_<name>/README.md` with only **Features** and **Limitations**.
6. Add focused tests for metadata, config/form parsing, generated queries, and non-live behavior.

## Boundaries

- No concrete-driver checks in `dbflux_ui` or app workflow code.
- No mock final behavior unless the user explicitly asks for a demo/test scaffold.
- Unsupported operations must return explicit `DbError::NotSupported` or equivalent errors.
- Do not log passwords, tokens, session credentials, or secret field values.

## Commands

```bash
cargo fmt --all -- --check
cargo check -p dbflux --features <driver-feature>
cargo test -p dbflux_driver_<name>
cargo check --workspace
```
