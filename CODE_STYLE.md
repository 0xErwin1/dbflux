# Code Style

## Naming Conventions

| Element                   | Convention           | Examples                                    | References                                                      |
| ------------------------- | -------------------- | ------------------------------------------- | --------------------------------------------------------------- |
| Types (struct/enum/trait) | PascalCase           | `ConnectionProfile`, `DbKind`               | crates/dbflux_core/src/connection/profile.rs                               |
| Functions/methods         | snake_case           | `prepare_connect_profile`, `refresh_schema` | crates/dbflux/src/app.rs, crates/dbflux/src/ui/views/workspace/mod.rs |
| Fields/locals             | snake_case           | `active_connection_id`, `pending_command`   | crates/dbflux/src/app.rs, crates/dbflux/src/ui/views/workspace/mod.rs |
| Constants                 | UPPER_SNAKE_CASE     | `MAX_VISIBLE`, `ROW_COMPACT`                | crates/dbflux/src/ui/tokens.rs, crates/dbflux/src/keymap/defaults.rs |
| Tests                     | `test_` + snake_case | `test_parse_simple_key`                     | crates/dbflux/src/keymap/chord.rs                               |

## File Organization

- Workspace crates live under `crates/`, with UI in `crates/dbflux/` and shared domain logic in `crates/dbflux_core/` (Cargo.toml).
- Module directories use `mod.rs` (e.g., `core/mod.rs`). `dbflux_core` is organized into thematic subdirectories: `core/`, `driver/`, `schema/`, `sql/`, `query/`, `connection/`, `storage/`, `data/`, `config/`, `facade/`.
- UI is organized by pane, window, and component in `crates/dbflux/src/ui/` (workspace, sidebar, editor, dock, document, windows, components).
- Drivers and supporting libraries live in their own crates (`crates/dbflux_driver_postgres/`, `crates/dbflux_driver_sqlite/`, `crates/dbflux_driver_mysql/`, `crates/dbflux_driver_mongodb/`, `crates/dbflux_driver_redis/`, `crates/dbflux_driver_dynamodb/`, `crates/dbflux_aws/`, `crates/dbflux_ssm/`, `crates/dbflux_ipc/`, `crates/dbflux_driver_ipc/`, `crates/dbflux_driver_host/`, `crates/dbflux_tunnel_core/`, `crates/dbflux_proxy/`, `crates/dbflux_ssh/`, `crates/dbflux_export/`, `crates/dbflux_test_support/`).

## Import Style

- Imports are grouped at the top with braces for multi-item paths (crates/dbflux/src/app.rs, crates/dbflux/src/ui/views/workspace/mod.rs).
- Internal modules prefer `crate::` and `super::` paths for local imports (crates/dbflux/src/ui/views/workspace/mod.rs).
- External dependencies are listed separately from internal modules in the import block.

## Code Patterns

- GPUI entities: stateful UI pieces are `Entity<T>` values; updates use `entity.update(cx, |state, cx| { ... })` (crates/dbflux/src/ui/views/workspace/mod.rs).
- Background work: use `cx.background_executor().spawn(...)` for DB/IO, then `cx.spawn(...).update(...)` to re-enter UI thread (crates/dbflux/src/ui/views/workspace/mod.rs).
- Feature gating: drivers are compiled via `#[cfg(feature = "sqlite")]` / `#[cfg(feature = "postgres")]` (crates/dbflux/src/app.rs).
- External RPC drivers are registered with `rpc:<socket_id>` keys and use `DbConfig::External { kind, values }` for persisted profile config (crates/dbflux/src/app.rs, crates/dbflux_core/src/connection/profile.rs).
- RPC protocol schemas and DTO conversions stay in `dbflux_ipc`; transport/client logic stays in `dbflux_driver_ipc` (crates/dbflux_ipc/src/driver_protocol.rs, crates/dbflux_driver_ipc/src/transport.rs).
- Default constructors and helpers use `new`/`default_*` naming (crates/dbflux_core/src/connection/profile.rs, crates/dbflux_core/src/query/types.rs).
- Results and metadata are plain structs/enums with small helper methods (crates/dbflux_core/src/query/types.rs, crates/dbflux_core/src/schema/types.rs).
- Generic store/manager: `JsonStore<T>` provides file persistence; type aliases (`ProfileStore`, `ProxyStore`) use named constructors (`.profiles()`, `.proxies()`). `ItemManager<T>` adds CRUD + auto-save; concrete managers are type aliases with `DefaultFilename` for `Default` impl (crates/dbflux_core/src/storage/json_store.rs, crates/dbflux_core/src/connection/item_manager.rs).
- Trait-based deduplication: `HasSecretRef` unifies keyring operations, `Identifiable` unifies ID access. Prefer a shared trait + generic method over per-type copy-paste (crates/dbflux_core/src/storage/secret_manager.rs, crates/dbflux_core/src/connection/item_manager.rs).
- Callback injection for cross-crate boundaries: `CreateTunnelFn` avoids circular dependency by defining a function signature in `dbflux_core` and supplying the real implementation from the app crate (crates/dbflux_core/src/connection/manager.rs, crates/dbflux/src/proxy.rs).
- Reusable navigation components: `TreeNav` (plain struct, not Entity) for tree navigation; `FormGridNav<F>` for 2D grid form navigation. Both take dynamic state as input rather than storing it (crates/dbflux/src/ui/components/tree_nav/mod.rs, crates/dbflux/src/ui/windows/settings/form_nav.rs).
- Shared process execution: process-backed hooks and `dbflux.process.run()` should reuse `dbflux_core::execute_streaming_process()` instead of maintaining separate polling or output-capture loops.
- Live script output: prefer a channel plus a document-owned buffer (`LiveOutputState`) for streamed UI output; do not use shared `Arc<Mutex<String>>` buffers for live rendering.
- Script languages (`Lua`, `Python`, `Bash`) are handled by `CodeDocument`; they execute as scripts, not DB queries, and should not depend on connection context UI.

## Error Handling

- Domain errors use `DbError` and `Result<T, DbError>` (crates/dbflux_core/src/core/error.rs, crates/dbflux_core/src/core/traits.rs).
- App-level operations log failures and continue with fallback state (crates/dbflux/src/app.rs).
- Avoid panics; use `?`, `map_err`, and logged errors. Panics only appear in startup `expect` calls (crates/dbflux/src/main.rs).
- Cancellation and unsupported features return explicit errors (crates/dbflux_core/src/core/traits.rs).

## Logging

- Use `log::{info, warn, error, debug}` throughout app and driver layers (crates/dbflux/src/app.rs, crates/dbflux/src/ui/views/workspace/mod.rs).
- Logging is initialized via `env_logger` in the app entry point (crates/dbflux/src/main.rs).

## Testing

- Unit tests live beside implementation in `#[cfg(test)] mod tests` blocks (crates/dbflux/src/keymap/chord.rs).
- Tests use `#[test]` and `assert_eq!`/`assert!` with snake_case names.
- Integration tests for drivers use Docker containers managed by `dbflux_test_support`; run ignored suites explicitly with `cargo test -p <crate> --test live_integration -- --ignored`.
- Test-only constructors (e.g., `ItemManager::with_store`) are gated behind `#[cfg(test)]`.
- For large GPUI-heavy modules, extract pure state helpers into small modules when that keeps unit tests simple and avoids bloating the main document module.

## Do's and Don'ts

- Do call `cx.notify()` after state changes that should re-render (AGENTS.md, crates/dbflux/src/ui/views/workspace/mod.rs).
- Do use `background_executor().spawn()` for DB operations to avoid blocking UI (AGENTS.md).
- Do propagate or log errors; do not silently discard fallible results (AGENTS.md).
- Do refactor and modularize functions that grow beyond ~100 lines; treat this as a design smell.
- Do use abstractions (`DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`) to adapt UI behavior instead of driver-specific conditionals.
- Do keep each `crates/dbflux_driver_*/README.md` updated with current **Features** and **Limitations**.
- Do use `LuaCapabilities::all_enabled()` for editor-run Lua scripts so the script runner matches the full hook-testing environment.
- Do treat external service `Hello` metadata/form definition as the source of truth for RPC drivers.
- Do use `mod.rs` for module directories (e.g., `core/mod.rs`, not a sibling `core.rs`) (AGENTS.md).
- Don't use deprecated GPUI types (`Model<T>`, `View<T>`, etc.) (AGENTS.md).
- Don't add driver-specific logic in UI code (e.g., `if driver == "mongodb"`). Use capability flags and metadata from `DriverMetadata` instead.
- Don't import driver crates directly in UI code. All driver interaction goes through `dbflux_core` traits.
- Don't add a second subprocess execution path for hooks or Lua helpers when the shared streaming executor already fits the job.
- Don't use `config.json` to define driver metadata/forms for external services; it is runtime launch/socket config only.
- Do use type-erased handles (`Box<dyn Any + Send + Sync>`) when storing cross-crate RAII objects to avoid circular dependencies.
- Do use the `TunnelConnector` trait for new tunnel protocols instead of duplicating RAII/lifecycle logic.
- Don't combine proxy and SSH tunnel on the same connection (mutually exclusive, enforced in `ConnectProfileParams::execute()`).
