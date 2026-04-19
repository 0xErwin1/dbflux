# Code Style

## Naming Conventions

| Element                   | Convention           | Examples                                    | References                                                      |
| ------------------------- | -------------------- | ------------------------------------------- | --------------------------------------------------------------- |
| Types (struct/enum/trait) | PascalCase           | `ConnectionProfile`, `DbKind`               | crates/dbflux_core/src/connection/profile.rs                               |
| Functions/methods         | snake_case           | `prepare_connect_profile`, `refresh_schema` | crates/dbflux_app/src/app_state.rs, crates/dbflux_ui/src/ui/views/workspace/mod.rs |
| Fields/locals             | snake_case           | `active_connection_id`, `pending_command`   | crates/dbflux_app/src/app_state.rs, crates/dbflux_ui/src/ui/views/workspace/mod.rs |
| Constants                 | UPPER_SNAKE_CASE     | `MAX_VISIBLE`, `ROW_COMPACT`                | crates/dbflux_ui/src/ui/tokens.rs, crates/dbflux_ui/src/keymap/defaults.rs |
| Tests                     | `test_` + snake_case | `test_parse_simple_key`                     | crates/dbflux_ui/src/keymap/chord.rs                               |

## File Organization

- The canonical repo layout, crate map, and key-file overview live in `ARCHITECTURE.md`.
- Workspace crates live under `crates/`. In practice, most changes land in `dbflux_ui`, `dbflux_app`, `dbflux_core`, or one of the `dbflux_driver_*` crates.
- Module directories use `mod.rs`.
- Keep new code in the existing crate and area that already owns the behavior instead of creating parallel structure.

## Import Style

- Imports are grouped at the top with braces for multi-item paths (crates/dbflux_app/src/app_state.rs, crates/dbflux_ui/src/ui/views/workspace/mod.rs).
- Internal modules prefer `crate::` and `super::` paths for local imports (crates/dbflux_ui/src/ui/views/workspace/mod.rs).
- External dependencies are listed separately from internal modules in the import block.

## Code Patterns

- GPUI entities: stateful UI pieces are `Entity<T>` values; updates use `entity.update(cx, |state, cx| { ... })` (crates/dbflux_ui/src/ui/views/workspace/mod.rs).
- Background work: use `cx.background_executor().spawn(...)` for DB/IO, then `cx.spawn(...).update(...)` to re-enter UI thread (crates/dbflux_ui/src/ui/views/workspace/mod.rs).
- Feature gating: drivers are compiled via `#[cfg(feature = "sqlite")]` / `#[cfg(feature = "postgres")]` (crates/dbflux_app/src/app_state.rs).
- External RPC drivers are registered with `rpc:<socket_id>` keys and use `DbConfig::External { kind, values }` for persisted profile config (crates/dbflux_app/src/app_state.rs, crates/dbflux_core/src/connection/profile.rs).
- RPC protocol schemas and DTO conversions stay in `dbflux_ipc`; transport/client logic stays in `dbflux_driver_ipc` (crates/dbflux_ipc/src/driver_protocol.rs, crates/dbflux_driver_ipc/src/transport.rs).
- Default constructors and helpers use `new`/`default_*` naming (crates/dbflux_core/src/connection/profile.rs, crates/dbflux_core/src/query/types.rs).
- Results and metadata are plain structs/enums with small helper methods (crates/dbflux_core/src/query/types.rs, crates/dbflux_core/src/schema/types.rs).
- Generic store/manager: `JsonStore<T>` provides file persistence; type aliases (`ProfileStore`, `ProxyStore`) use named constructors (`.profiles()`, `.proxies()`). `ItemManager<T>` adds CRUD + auto-save; concrete managers are type aliases with `DefaultFilename` for `Default` impl (crates/dbflux_core/src/storage/json_store.rs, crates/dbflux_core/src/connection/item_manager.rs).
- Trait-based deduplication: `HasSecretRef` unifies keyring operations, `Identifiable` unifies ID access. Prefer a shared trait + generic method over per-type copy-paste (crates/dbflux_core/src/storage/secret_manager.rs, crates/dbflux_core/src/connection/item_manager.rs).
- Callback injection for cross-crate boundaries: `CreateTunnelFn` avoids circular dependency by defining a function signature in `dbflux_core` and supplying the real implementation from the app crate (crates/dbflux_core/src/connection/manager.rs, crates/dbflux_app/src/proxy.rs).
- Reusable navigation components: `TreeNav` (plain struct, not Entity) for tree navigation; `FormGridNav<F>` for 2D grid form navigation. Both take dynamic state as input rather than storing it (crates/dbflux_ui/src/ui/components/tree_nav/mod.rs, crates/dbflux_ui/src/ui/windows/settings/form_nav.rs).
- Shared process execution: process-backed hooks and `dbflux.process.run()` should reuse `dbflux_core::execute_streaming_process()` instead of maintaining separate polling or output-capture loops.
- Live script output: prefer a channel plus a document-owned buffer (`LiveOutputState`) for streamed UI output; do not use shared `Arc<Mutex<String>>` buffers for live rendering.
- Script languages (`Lua`, `Python`, `Bash`) are handled by `CodeDocument`; they execute as scripts, not DB queries, and should not depend on connection context UI.
- Driver-owned query generation: textual read/DML previews, copy-as-query flows, and MCP mutation/query previews should go through `Connection::query_generator()` instead of ad hoc UI-side SQL building. Keep `CodeGenerator` focused on DDL.
- Platform detection: use `platform::floating_window_kind()` for secondary windows (Settings, Connection Manager); use `platform::apply_window_options()` to set min size for X11 compatibility with tiling WMs.
- MCP governance: all MCP operations go through `McpGovernanceService` trait; policy decisions use `PolicyEngine::evaluate()` with `ExecutionClassification` and return `PolicyDecision`.
- Settings sections: implement `SettingsSection` trait for keyboard navigation; use `FormSection` trait for form-based sections with 2D grid navigation.
- Multi-select dropdowns: use `MultiSelect` component for multi-value selection; emits `MultiSelectChanged` on selection change.
- Value source selection: use `ValueSourceSelector` for fields that can be literal, env var, secret, parameter, or auth session field.

## Error Handling

- Domain errors use `DbError` and `Result<T, DbError>` (crates/dbflux_core/src/core/error.rs, crates/dbflux_core/src/core/traits.rs).
- App-level operations log failures and continue with fallback state (crates/dbflux_app/src/app_state.rs).
- Avoid panics; use `?`, `map_err`, and logged errors. Panics only appear in startup `expect` calls (crates/dbflux/src/main.rs).
- Cancellation and unsupported features return explicit errors (crates/dbflux_core/src/core/traits.rs).

## Logging

- Use `log::{info, warn, error, debug}` throughout app and driver layers (crates/dbflux_app/src/app_state.rs, crates/dbflux_ui/src/ui/views/workspace/mod.rs).
- Logging is initialized via `env_logger` in the app entry point (crates/dbflux/src/main.rs).

## Testing

- Unit tests live beside implementation in `#[cfg(test)] mod tests` blocks (crates/dbflux_ui/src/keymap/chord.rs).
- Tests use `#[test]` and `assert_eq!`/`assert!` with snake_case names.
- Integration tests for drivers use Docker containers managed by `dbflux_test_support`; run ignored suites explicitly with `cargo test -p <crate> --test live_integration -- --ignored`.
- Test-only constructors (e.g., `ItemManager::with_store`) are gated behind `#[cfg(test)]`.
- For large GPUI-heavy modules, extract pure state helpers into small modules when that keeps unit tests simple and avoids bloating the main document module.

## Do's and Don'ts

- Do call `cx.notify()` after state changes that should re-render (AGENTS.md, crates/dbflux_ui/src/ui/views/workspace/mod.rs).
- Do use `background_executor().spawn()` for DB operations to avoid blocking UI (AGENTS.md).
- Do propagate or log errors; do not silently discard fallible results (AGENTS.md).
- Do refactor and modularize functions that grow beyond ~100 lines; treat this as a design smell.
- Do use abstractions (`DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`) to adapt UI behavior instead of driver-specific conditionals.
- Do use `QueryGenerator` for copied queries and textual read/DML previews; do not duplicate mutation or read template formatting in the UI.
- Do keep each `crates/dbflux_driver_*/README.md` updated with current **Features** and **Limitations**.
- Do use `LuaCapabilities::all_enabled()` for editor-run Lua scripts so the script runner matches the full hook-testing environment.
- Do treat external service `Hello` metadata/form definition as the source of truth for RPC drivers.
- Do use `mod.rs` for module directories (e.g., `core/mod.rs`, not a sibling `core.rs`) (AGENTS.md).
- Don't use deprecated GPUI types (`Model<T>`, `View<T>`, etc.) (AGENTS.md).
- Don't add driver-specific logic in UI code (e.g., `if driver == "mongodb"`). Use capability flags and metadata from `DriverMetadata` instead.
- Don't import driver crates directly in UI code. All driver interaction goes through `dbflux_core` traits.
- Don't route DML or read query templates through `CodeGenerator`; reserve `CodeGenerator` for DDL.
- Don't add a second subprocess execution path for hooks or Lua helpers when the shared streaming executor already fits the job.
- Don't use `config.json` to define driver metadata/forms for external services; it is runtime launch/socket config only.
- Do use type-erased handles (`Box<dyn Any + Send + Sync>`) when storing cross-crate RAII objects to avoid circular dependencies.
- Do use the `TunnelConnector` trait for new tunnel protocols instead of duplicating RAII/lifecycle logic.
- Don't combine proxy and SSH tunnel on the same connection (mutually exclusive, enforced in `ConnectProfileParams::execute()`).
- Do use `ExecutionClassification` to categorize operations for MCP policy decisions (Metadata/Read/Write/Destructive/Admin).
- Do implement `SettingsSection` trait for settings sections that need keyboard navigation and dirty-state tracking.
- Do use `FormSection` trait for form-based settings sections with 2D grid navigation and blur handling.
- Do use `McpGovernanceService` trait for MCP-related governance operations; do not bypass policy engine.
- Do use `platform::floating_window_kind()` for secondary windows to avoid X11 transient dialog issues.
- Do log all MCP policy decisions via `AuditService` for compliance and debugging.
- Do emit audit events through `EventSink` trait rather than calling `AuditService` directly in service layers; this decouples services from the storage implementation.
- Do use canonical action string constants from `dbflux_core::observability::actions` instead of bare string literals in audit events.
- Do set category-specific required fields before calling `EventSink::record()` — validation runs at record time and returns an error if fields are missing.
- Do not store full query text in `details_json` — the `AuditService` replaces it with a SHA256 fingerprint by default; if full text is needed, opt in explicitly with `set_capture_query_text(true)` and understand the compliance implications.
