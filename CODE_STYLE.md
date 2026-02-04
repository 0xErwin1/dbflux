# Code Style

## Naming Conventions

| Element                   | Convention           | Examples                                    | References                                                      |
| ------------------------- | -------------------- | ------------------------------------------- | --------------------------------------------------------------- |
| Types (struct/enum/trait) | PascalCase           | `ConnectionProfile`, `DbKind`               | crates/dbflux_core/src/profile.rs                               |
| Functions/methods         | snake_case           | `prepare_connect_profile`, `refresh_schema` | crates/dbflux/src/app.rs, crates/dbflux/src/ui/workspace.rs     |
| Fields/locals             | snake_case           | `active_connection_id`, `pending_command`   | crates/dbflux/src/app.rs, crates/dbflux/src/ui/workspace.rs     |
| Constants                 | UPPER_SNAKE_CASE     | `MAX_VISIBLE`, `ROW_COMPACT`                | crates/dbflux/src/ui/history.rs, crates/dbflux/src/ui/tokens.rs |
| Tests                     | `test_` + snake_case | `test_parse_simple_key`                     | crates/dbflux/src/keymap/chord.rs                               |

## File Organization

- Workspace crates live under `crates/`, with UI in `crates/dbflux/` and shared domain logic in `crates/dbflux_core/` (Cargo.toml).
- Each module is a dedicated file (no `mod.rs`); submodules are declared in the parent file (AGENTS.md, crates/dbflux/src/ui/mod.rs).
- UI is organized by pane, window, and component in `crates/dbflux/src/ui/` (workspace, sidebar, editor, dock, document, windows, components).
- Drivers and supporting libraries live in their own crates (`crates/dbflux_driver_postgres/`, `crates/dbflux_driver_sqlite/`, `crates/dbflux_ssh/`, `crates/dbflux_export/`).

## Import Style

- Imports are grouped at the top with braces for multi-item paths (crates/dbflux/src/app.rs, crates/dbflux/src/ui/workspace.rs).
- Internal modules prefer `crate::` and `super::` paths for local imports (crates/dbflux/src/ui/workspace.rs).
- External dependencies are listed separately from internal modules in the import block.

## Code Patterns

- GPUI entities: stateful UI pieces are `Entity<T>` values; updates use `entity.update(cx, |state, cx| { ... })` (crates/dbflux/src/ui/workspace.rs).
- Background work: use `cx.background_executor().spawn(...)` for DB/IO, then `cx.spawn(...).update(...)` to re-enter UI thread (crates/dbflux/src/ui/workspace.rs).
- Feature gating: drivers are compiled via `#[cfg(feature = "sqlite")]` / `#[cfg(feature = "postgres")]` (crates/dbflux/src/app.rs).
- Default constructors and helpers use `new`/`default_*` naming (crates/dbflux_core/src/profile.rs, crates/dbflux_core/src/query.rs).
- Results and metadata are plain structs/enums with small helper methods (crates/dbflux_core/src/query.rs, crates/dbflux_core/src/schema.rs).

## Error Handling

- Domain errors use `DbError` and `Result<T, DbError>` (crates/dbflux_core/src/error.rs, crates/dbflux_core/src/traits.rs).
- App-level operations log failures and continue with fallback state (crates/dbflux/src/app.rs).
- Avoid panics; use `?`, `map_err`, and logged errors. Panics only appear in startup `expect` calls (crates/dbflux/src/main.rs).
- Cancellation and unsupported features return explicit errors (crates/dbflux_core/src/traits.rs).

## Logging

- Use `log::{info, warn, error, debug}` throughout app and driver layers (crates/dbflux/src/app.rs, crates/dbflux/src/ui/workspace.rs).
- Logging is initialized via `env_logger` in the app entry point (crates/dbflux/src/main.rs).

## Testing

- Unit tests live beside implementation in `#[cfg(test)] mod tests` blocks (crates/dbflux/src/keymap/chord.rs).
- Tests use `#[test]` and `assert_eq!`/`assert!` with snake_case names.

## Do's and Don'ts

- Do call `cx.notify()` after state changes that should re-render (AGENTS.md, crates/dbflux/src/ui/workspace.rs).
- Do use `background_executor().spawn()` for DB operations to avoid blocking UI (AGENTS.md).
- Do propagate or log errors; do not silently discard fallible results (AGENTS.md).
- Do use abstractions (`DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`) to adapt UI behavior instead of driver-specific conditionals.
- Don't create `mod.rs` files; declare modules directly in `src/*.rs` (AGENTS.md).
- Don't use deprecated GPUI types (`Model<T>`, `View<T>`, etc.) (AGENTS.md).
- Don't add driver-specific logic in UI code (e.g., `if driver == "mongodb"`). Use capability flags and metadata from `DriverMetadata` instead.
- Don't import driver crates directly in UI code. All driver interaction goes through `dbflux_core` traits.
