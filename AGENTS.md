# AGENTS.md — DBFlux

Guidelines for AI agents working in this Rust/GPUI codebase.

## Project Overview

DBFlux is a keyboard-first database client built with Rust and GPUI (Zed's UI framework).

**Workspace structure:**

```
crates/
├── dbflux/                    # Binary shell: main entry point, CLI, IPC server
├── dbflux_ui/                 # GPUI UI layer: views, documents, overlays, components, keymap
├── dbflux_app/                # Runtime/domain: AppState, managers, hooks, auth providers
├── dbflux_core/               # Traits, types, errors, driver capabilities (stable API)
├── dbflux_ipc/                # IPC envelopes, framing, and driver RPC protocol
├── dbflux_driver_ipc/         # External driver proxy over local IPC
├── dbflux_driver_host/        # RPC host process for out-of-process drivers
├── dbflux_driver_postgres/    # PostgreSQL driver
├── dbflux_driver_sqlite/      # SQLite driver
├── dbflux_driver_mysql/       # MySQL/MariaDB driver
├── dbflux_driver_mongodb/     # MongoDB driver
├── dbflux_driver_redis/       # Redis driver
├── dbflux_driver_dynamodb/    # DynamoDB driver
├── dbflux_aws/                # AWS auth providers + AWS value providers
├── dbflux_ssm/                # AWS SSM managed tunnel factory
├── dbflux_lua/                # Embedded Lua runtime for in-process hooks
├── dbflux_tunnel_core/        # Shared RAII tunnel infrastructure (proxy + SSH)
├── dbflux_proxy/              # SOCKS5/HTTP CONNECT proxy tunnel
├── dbflux_ssh/                # SSH tunnel support
├── dbflux_export/             # Export (CSV, JSON, Text, Binary)
├── dbflux_test_support/       # Docker containers and fixtures for integration tests
├── dbflux_mcp/                # MCP runtime, governance service, and tool catalog
├── dbflux_mcp_server/         # Standalone MCP server binary for AI clients
├── dbflux_policy/             # Policy engine, roles, trusted clients, classification
├── dbflux_approval/           # Approval service and pending execution store
├── dbflux_audit/              # Audit logging with SQLite backend
└── dbflux_storage/            # Unified storage: SQLite database, migrations, repositories
```

## Build & Run Commands

```bash
cargo check --workspace              # Fast type checking
cargo build -p dbflux --features sqlite,postgres,mysql,mongodb,redis,dynamodb,aws  # Debug build
cargo build -p dbflux --features sqlite,postgres,mysql,mongodb,redis,dynamodb,aws --release  # Release build
cargo run -p dbflux --features sqlite,postgres,mysql,mongodb,redis,dynamodb,aws    # Run app

# MCP server (AI integration) - included by default
cargo build -p dbflux  # MCP included in default features
./target/debug/dbflux mcp --client-id test-client

# Build without MCP support (smaller binary, no AI integration)
cargo build -p dbflux --no-default-features --features sqlite,postgres,mysql,mongodb,redis,dynamodb,lua,aws

cargo fmt --all                      # Format
cargo clippy --workspace -- -D warnings  # Lint
cargo test --workspace               # All tests
cargo test --workspace test_name     # Single test
cargo test -p dbflux_core            # Tests in specific crate
cargo test -p dbflux_driver_dynamodb --test live_integration -- --ignored  # Docker-backed live tests

# Nix
nix develop                          # Enter dev shell
nix build                            # Build package
nix run                              # Run directly
```

## Rust Guidelines

### General Principles

- Prioritize correctness and clarity over speed
- Do not write comments that summarize code; only explain non-obvious "why"
- Prefer implementing in existing files unless it's a new logical component
- Avoid creating many small files
- Avoid creative additions unless explicitly requested
- Use full words for variable names (no abbreviations like "q" for "queue")

### Error Handling

- Avoid `unwrap()` and functions that panic; use `?` to propagate errors
- Be careful with indexing operations that may panic on out-of-bounds
- Never silently discard errors with `let _ =` on fallible operations:
  - Propagate with `?` when the caller should handle them
  - Use `.log_err()` when ignoring but wanting visibility
  - Use `match` or `if let Err(...)` for custom logic
- Ensure async errors propagate to UI so users get meaningful feedback

### File Organization

- Use `mod.rs` for module directories (e.g., `views/mod.rs`, not a sibling `views.rs`)
- When creating crates, specify library root in `Cargo.toml` with `[lib] path = "..."`

### Async Patterns

Use variable shadowing to scope clones in async contexts:

```rust
executor.spawn({
    let task_ran = task_ran.clone();
    async move {
        *task_ran.borrow_mut() = true;
    }
});
```

### Performance Patterns

**Pre-compute expensive operations**: Move string formatting and allocation into constructors rather than during rendering:

```rust
// Good: Format once during construction
CellValue::Text { display: format!("{}", value), ... }

// Bad: Format on every render
fn render(&self) { format!("{}", self.value) }
```

**Lazy loading for large datasets**: Drivers should return shallow metadata initially and fetch details on-demand:

```rust
fn get_tables(&self) -> Vec<TableInfo> // Names only
fn table_details(&self, name: &str) -> TableDetails // Columns, indexes
```

**Driver error formatting**: Drivers implement the `ErrorFormatter` trait from `dbflux_core/src/core/error_formatter.rs` to extract detailed error info. PostgreSQL's `as_db_error()` provides detail, hint, column, table, and constraint fields. MongoDB extracts error codes and labels. Use structured error formatting instead of raw `format!("{:?}", e)`.

## GPUI Guidelines

### Context Types

- `App` — root context for global state and entity access
- `Context<T>` — provided when updating `Entity<T>`, derefs to `App`
- `AsyncApp` / `AsyncWindowContext` — from `cx.spawn`, can cross await points
- `Window` — window state, passed before `cx` when present

### Entity Operations

With `thing: Entity<T>`:

- `thing.read(cx)` → `&T`
- `thing.update(cx, |thing, cx| ...)` → mutate with `Context<T>`
- `thing.update_in(cx, |thing, window, cx| ...)` → also provides `Window`

Use the inner `cx` inside closures, not the outer one, to avoid multiple borrows.

### Concurrency

All entity/UI work happens on the foreground thread.

```rust
// Background work + foreground update
let task = cx.background_executor().spawn(async move {
    // expensive work
});

cx.spawn(async move |_this, cx| {
    let result = task.await;
    cx.update(|cx| {
        entity.update(cx, |state, cx| {
            state.pending_result = Some(result);
            cx.notify();
        });
    }).ok();
}).detach();
```

Task handling:

- Await in another async context
- `task.detach()` or `task.detach_and_log_err(cx)` for fire-and-forget
- Store in a field if work should cancel when struct drops

### Rendering

Types implement `Render` for element trees with flexbox layout:

```rust
impl Render for MyComponent {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().border_1().child("Hello")
    }
}
```

- Use `.when(condition, |this| ...)` for conditional attributes/children
- Use `.when_some(option, |this, value| ...)` for Option-based conditionals
- Call `cx.notify()` when state changes affect rendering

### Entity Updates in Render

Use `pending_*` fields with `.take()` to safely update other entities or open modals:

```rust
fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
    if let Some(data) = self.pending_data.take() {
        self.other_entity.update(cx, |other, cx| {
            other.apply(data, window, cx);
        });
    }
    // For modals: defer open until render
    if let Some(modal) = self.pending_modal_open.take() {
        self.modal.update(cx, |m, cx| m.open(modal.value, window, cx));
    }
    // render UI...
}
```

### Input & Actions

Event handlers: `.on_click(cx.listener(|this, event, window, cx| ...))`

Actions defined with `actions!(namespace, [SomeAction])` macro or `#[derive(Action)]`.

### Keyboard & Mouse Patterns

**Focus tracking**: Use `.track_focus(&focus_handle)` on container elements to receive key events:

```rust
div()
    .track_focus(&self.focus_handle)
    .on_key_down(cx.listener(|this, event, window, cx| { ... }))
    .child(content)
```

**Mouse/keyboard sync**: When a component supports both mouse and keyboard navigation, sync state on mouse events:

```rust
.on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
    this.focus_mode = FocusMode::SomeMode;
    this.edit_state = EditState::Editing;
    cx.notify();
}))
```

**Input blur race condition**: When switching between inputs via click, the old input's `Blur` event fires after the new input's `mousedown`. Use a flag to prevent focus theft:

```rust
// In mousedown handler
this.switching_input = true;

// In blur handler / exit_edit_mode
if self.switching_input {
    self.switching_input = false;
    return;
}
```

**Focus state machines**: For complex focus scenarios (e.g., toolbar with editable inputs), use explicit state enums:

```rust
enum FocusMode { Table, Toolbar }
enum EditState { Navigating, Editing }
```

### Subscriptions

```rust
cx.subscribe(other_entity, |this, other_entity, event, cx| ...)
```

Returns `Subscription`; store in `_subscriptions: Vec<Subscription>` field.

### Deprecated Types (NEVER use)

- `Model<T>`, `View<T>` → use `Entity<T>`
- `AppContext` → use `App`
- `ModelContext<T>` → use `Context<T>`
- `WindowContext`, `ViewContext<T>` → use `Window` + `Context<T>`

## Architecture Rules

### Crate Boundaries

- `dbflux`: Binary shell — `main.rs`, `cli.rs`, single-instance IPC, window bootstrap. Does NOT contain UI or domain logic.
- `dbflux_ui`: GPUI UI layer — all views, documents, overlays, components, windows, keymap (GPUI-bound parts), `AppStateEntity` wrapper, `ipc_server`, `assets`, `platform`. Depends on `dbflux_app` and `dbflux_core`.
- `dbflux_app`: Runtime/domain — `AppState` (plain struct), `AppAccessManager`, `CompositeExecutor`, `AuthProviderRegistry`, config loader, history manager, `mcp_command`. **Zero GPUI dependency.** Depends on `dbflux_core`, `dbflux_storage`, drivers, audit, policy, MCP.
- `dbflux_core`: Pure types/traits, driver capabilities, SQL generation, query generator trait, no DB-specific code
- `dbflux_ipc`: Versioned app-control + driver RPC protocol contracts, framing, socket naming helpers
- `dbflux_driver_ipc`: RPC client transport and `DbDriver` adapter for external services
- `dbflux_driver_host`: Standalone RPC host binary that serves drivers over local sockets
- `dbflux_driver_*`: Implement `DbDriver`, `Connection`, `ErrorFormatter`, and query generation abstractions (`QueryGenerator` for mutation/read templates when applicable)
- `dbflux_aws`: AWS auth providers (SSO/shared/static), AWS account discovery, and AWS value providers
- `dbflux_ssm`: Managed AWS SSM port-forward tunnel factory used by `AccessManager`
- `dbflux_tunnel_core`: RAII `Tunnel`, `TunnelConnector` trait, `ForwardingConnection<R>` bidirectional forwarder, adaptive sleep
- `dbflux_proxy`: SOCKS5/HTTP CONNECT proxy via `TunnelConnector` impl
- `dbflux_ssh`: SSH tunnel via `TunnelConnector` impl (all SSH ops serialized to one thread for libssh2 safety)
- `dbflux_lua`: Embedded Lua runtime and `HookExecutor` implementation for in-process hooks
- `dbflux_mcp`: MCP runtime (`McpRuntime`), governance service trait (`McpGovernanceService`), tool catalog, and handlers for query/approval/discovery
- `dbflux_mcp_server`: Standalone binary for AI clients to connect via MCP protocol; uses `dbflux_mcp` runtime
- `dbflux_policy`: `PolicyEngine` with roles (`PolicyRole`) and tool policies (`ToolPolicy`); `TrustedClientRegistry`; `ExecutionClassification` enum
- `dbflux_approval`: `ApprovalService` and `InMemoryPendingExecutionStore` for deferred executions requiring human approval
- `dbflux_audit`: `AuditService` with SQLite backend for audit event logging, querying, and export

### Proxy and SSH Tunnels

- Proxy and SSH tunnels share the RAII lifecycle from `dbflux_tunnel_core::Tunnel`
- `TunnelConnector` trait: `test_connection()` + `run_tunnel_loop()` — each protocol implements its own
- `ForwardingConnection<R>` handles bidirectional client↔remote forwarding; `R` is `TcpStream` for proxy, `ssh2::Channel` for SSH
- Proxy+SSH are mutually exclusive per connection (guard in `ConnectProfileParams::execute()`)
- `CreateTunnelFn` callback avoids circular dependency: `dbflux_core` defines the function signature, `dbflux` (app crate) supplies the real `dbflux_proxy` implementation
- Proxy tunnel handle is type-erased (`Box<dyn Any + Send + Sync>`) and stored in `ConnectedProfile` for RAII lifetime
- `host_matches_no_proxy()` follows curl/wget `NO_PROXY` semantics (wildcard, exact, suffix with/without leading dot)

### External RPC Drivers

- Treat the external service `Hello` payload as source of truth for `DbKind`, metadata, and form definition
- RPC services are stored in `cfg_services` table in `~/.local/share/dbflux/dbflux.db` (socket_id, command, args, env, timeout)
- Internal driver keys for external services are `rpc:<socket_id>`
- Use `DbConfig::External { kind, values }` for external driver profile configs
- Only managed hosts started by DBFlux are shut down automatically

### Auth, Access, and Connect Pipeline

- Auth providers are runtime-registered in `AuthProviderRegistry` (`crates/dbflux_app/src/auth_provider_registry.rs`) instead of hardcoded through provider enums
- `AuthProfile` is provider-agnostic (`provider_id` + `fields`) and includes compatibility migration from legacy AWS-only payloads
- Access method supports provider-agnostic managed mode via `AccessKind::Managed { provider, params }`
- Legacy SSM access JSON (`method = "ssm"`) is migrated transparently to managed access at deserialization time
- Connect execution runs through `dbflux_core::pipeline::run_pipeline` with staged progress (`Authenticating`, `ResolvingValues`, `OpeningAccess`)
- App-level access dispatch is centralized in `AppAccessManager` (`crates/dbflux_app/src/access_manager.rs`) and currently handles managed provider `aws-ssm`

### Driver/UI Decoupling

**Never add driver-specific logic in UI code.** The UI must remain agnostic to specific database implementations.

Instead of:

```rust
// BAD: Driver-specific conditional in UI
if driver_id == "mongodb" {
    show_document_view();
} else {
    show_table_view();
}
```

Use abstractions from `DriverMetadata`:

```rust
// GOOD: Use capability flags and metadata
match metadata.category {
    DatabaseCategory::Document => show_document_view(),
    DatabaseCategory::Relational => show_table_view(),
    _ => show_generic_view(),
}

// GOOD: Use query language for editor behavior
let placeholder = metadata.query_language.placeholder();
let editor_mode = metadata.query_language.editor_mode();
```

Key abstractions for UI adaptation:

- `DatabaseCategory`: Determines view mode (table vs document tree), terminology (rows vs documents)
- `QueryLanguage`: Determines editor syntax highlighting, placeholder text, comment prefix
- `DriverCapabilities`: Determines which features to enable (pagination, transactions, etc.)

### Generic Deduplication Patterns

**`JsonStore<T>`**: Single generic JSON-file store with type aliases (`ProfileStore`, `SshTunnelStore`, `ProxyStore`). Named constructors (`.profiles()`, `.ssh_tunnels()`, `.proxies()`) set the filename.

**`ItemManager<T>`**: CRUD manager with auto-save, backed by `JsonStore<T>`. Uses `Identifiable` trait for ID access and `DefaultFilename` trait for `Default` on type aliases. `ProxyManager` and `SshTunnelManager` are type aliases. `ProfileManager` stays separate (has extra methods like `find_by_id`, `profile_ids`).

**`HasSecretRef`**: Unifies secret operations for types with keyring references (`SshTunnelProfile`, `ProxyProfile`, `AuthProfile`). `SecretManager` generic methods (`get_secret`, `save_secret`, `delete_secret`) delegate through this trait.

**`FormGridNav<F>`**: 2D grid navigation for settings forms. Takes `&[Vec<F>]` rows as input to each method (not stored), so callers compute dynamic grids from their own state. Used by proxy and SSH tunnel settings forms.

**`TreeNav`**: Reusable tree navigation component (plain struct, not a GPUI Entity). Supports cursor movement, expand/collapse, select-by-id. Used by Settings sidebar and connections sidebar.

### Connection Hooks

- Hooks are reusable command definitions (name, command, args, cwd, env, timeout, failure policy)
- Hook execution modes are `Command`, `Script`, and `Lua`
- Process-backed hooks can be inline or file-backed; Lua hooks run in-process through `dbflux_lua`
- Profile phase bindings: PreConnect, PostConnect, PreDisconnect, PostDisconnect
- `HookRunner` orchestrates execution with `HookPhaseOutcome` (success/warning/abort)
- Each hook runs as its own background task with stdout/stderr visible in Tasks panel
- Process-backed hooks and `dbflux.process.run()` share the same streaming executor in `dbflux_core`; avoid duplicating process execution logic
- Editor-run Lua scripts use `LuaCapabilities::all_enabled()` and stream live output into a document-owned buffer via channel, not a shared mutex string
- Failure policies: Disconnect (abort flow), Warn (continue with warning), Ignore (log only)
- Hooks section in Settings for global definitions; Hooks tab in Connection Manager for per-profile bindings
- Types and logic in `dbflux_core/src/connection/hook.rs`, UI in `settings/hooks.rs` and `connection_manager/hooks_tab.rs`

### Adding a New Driver

1. Create `crates/dbflux_driver_<name>/`
2. Implement `DbDriver` and `Connection` from `dbflux_core`
3. Define `DriverMetadata` with appropriate `DatabaseCategory`, `QueryLanguage`, and `DriverCapabilities`
4. Implement `ErrorFormatter` for driver-specific error messages
5. Implement `QueryGenerator` when the driver can generate native mutation/read templates for UI previews, copy-as-query, or MCP previews
6. Add feature flag in `crates/dbflux/Cargo.toml`
7. Register in `AppState::new()` under `#[cfg(feature = "name")]`

### Driver Capabilities

Drivers declare their capabilities via `DriverMetadata`:

- `DatabaseCategory`: Relational, Document, KeyValue, Graph, TimeSeries, WideColumn
- `QueryLanguage`: SQL, MongoQuery, RedisCommands, Cypher, etc. (determines editor syntax highlighting and placeholder)
- `DriverCapabilities`: bitflags for features (PAGINATION, TRANSACTIONS, NESTED_DOCUMENTS, etc.)

### Driver README documentation

- Every driver crate under `crates/dbflux_driver_*/` must include a `README.md`.
- Keep each driver README focused on two sections: **Features** and **Limitations**.
- Update driver README files whenever capabilities, supported operations, or known limits change.

### Document System Pattern

Documents follow a consistent pattern for tab-based UI:

1. **Handle**: `DocumentHandle` wraps the entity and provides metadata
2. **State**: Document struct implements `Render` with internal focus management
3. **Tabs**: CodeDocument supports multiple result tabs with `TabManager`
4. **Scripts**: Lua/Python/Bash use the same document shell but execute as scripts, not DB queries; script output streams into `code/live_output.rs`
5. **Focus**: Documents receive `FocusTarget::Document` and manage internal focus
6. **Dedup**: Check for existing documents before creating new ones (e.g., `is_table()` for data documents)

### MCP Governance System

DBFlux supports the Model Context Protocol (MCP) for AI client integration with a complete governance layer:

**Classification**: Operations are classified by impact level via `ExecutionClassification`:
- `Metadata` — Schema introspection (list tables, describe object)
- `Read` — SELECT queries, data browsing
- `Write` — INSERT/UPDATE, mutations
- `Destructive` — DELETE, DROP, TRUNCATE
- `AdminSafe` — Safe DDL operations (CREATE TABLE, CREATE INDEX, ADD COLUMN with default/nullable)
- `Admin` — Risky DDL operations (DROP COLUMN, RENAME COLUMN, ALTER COLUMN, DROP INDEX)
- `AdminDestructive` — Irreversible DDL operations (DROP TABLE, DROP DATABASE, TRUNCATE TABLE)

**Policy Engine** (`dbflux_policy`):
- `PolicyEngine::evaluate()` takes actor, connection, tool, and classification
- Returns `PolicyDecision::Allow` or `PolicyDecision::Deny(reason)`
- Supports roles with policy composition and connection-scoped assignments
- `TrustedClientRegistry` identifies known AI clients

**Approval Flow** (`dbflux_approval`):
- Destructive or write operations can require human approval
- `InMemoryPendingExecutionStore` holds deferred executions
- `ApprovalService` manages approve/reject lifecycle

**Audit** (`dbflux_audit`):
- SQLite-backed audit log in `~/.local/share/dbflux/dbflux.db` (`aud_audit_events` table)
- Events use the `EventRecord` type from `dbflux_core::observability` with category, severity, outcome, actor type, and structured fields
- Events are emitted through the `EventSink` trait — inject `Arc<dyn EventSink>` into service layers rather than calling `AuditService` directly
- Categories: `Query`, `Connection`, `Hook`, `Script`, `Mcp`, `Governance`, `Config`, `System`
- By default: sensitive values are redacted, query text is replaced with a SHA256 fingerprint (not stored in full), details_json is capped at 64 KiB
- Queryable via `AuditQueryFilter` (actor, tool, category, action, outcome, date range, free text)
- Export to JSON/CSV via `AuditService::export()` (basic) or `export_extended()` (all fields including details_json)
- Purge old events by retention policy: `AuditService::purge_old_events(days, batch_size)`
- See `docs/AUDIT.md` for the full event schema, required fields per category, and usage patterns

**Runtime** (`dbflux_mcp`):
- `McpRuntime` implements `McpGovernanceService` trait
- Integrates policy engine, approval service, and audit service
- Emits `McpRuntimeEvent` for UI updates
- Tool catalog defines canonical MCP tools and deferred tools

**Important runtime rules**:
- `preview_mutation` must stay read-only; it may return generated SQL/query text or a non-mutating plan, but must never execute the mutation being previewed
- `preview_ddl` is intentionally not exposed from the MCP surface until DBFlux has a truly safe schema-preview path
- `select_data` must reject unsupported `joins` explicitly rather than ignoring them

**Standalone Server** (`dbflux_mcp_server`):
- Integrated as subcommand: `dbflux mcp --client-id <id>` for AI clients
- Communicates via JSON-RPC over stdin/stdout
- Uses same governance stack as in-app MCP
- Optional: Can be disabled with `--no-default-features` at build time

**UI Integration**:
- `McpApprovalsView` document for reviewing pending executions
- MCP settings section for trusted clients, roles, and policies
- `AuditDocument` as the unified audit viewer for all event categories (no separate MCP audit surface)
- `LoginModal` and `SsoWizard` overlays for AWS SSO authentication flow

### WHERE Clause Syntax

DBFlux MCP uses a unified JSON WHERE clause syntax that works across all database drivers (SQL, MongoDB, Redis, DynamoDB):

**ColumnRef Pattern**: Column references support three forms:
- `ColumnRef::Name("email")` — Simple column reference
- `ColumnRef::Nested(vec!["metadata", "profile", "age"])` — Nested document field (MongoDB, JSONB)
- `ColumnRef::JsonPath { column: "config", path: "$.notifications.email" }` — JSON path syntax

**Operators**: Standard comparison (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`), pattern matching (`$like`, `$ilike`, `$regex`), NULL handling (`null`, `$eq: null`), array operations (`$contains`, `$overlap`, `$size`, `$all`), and logical composition (`$and`, `$or`, `$not`).

**Type Coercion**: Automatic type conversion (string ↔ number ↔ boolean) with validation.

**Driver Translation**: WHERE clauses translate to SQL WHERE, MongoDB query filters, Redis SCAN patterns, or DynamoDB FilterExpression.

**Reference**: See `crates/dbflux_mcp_server/docs/WHERE_CLAUSE_SYNTAX.md` for complete syntax guide with examples.

### DDL Preview System

MCP provides a preview-before-execute workflow for schema changes:

**Preview Workflow**:
1. AI agent calls `preview_mutation` with operation parameters
2. DBFlux generates SQL/query text or an execution preview using driver-owned generation/planning
3. Preview returned with SQL, classification, affected objects, and warnings
4. Agent reviews and decides whether to proceed
5. Agent calls actual tool (`alter_table`, `create_table`, etc.) if safe

**Current limitation**:
- DDL preview is not exposed as a standalone MCP tool in this branch. The old `preview_ddl` surface was removed because it could not guarantee a non-mutating preview across drivers.

**Classification Algorithm**: `classify_alter_table_operation()` in `dbflux_core/src/query/classify.rs` determines risk level:
- `ADD COLUMN` (nullable or with default) → `AdminSafe`
- `ADD COLUMN` (non-nullable without default) → `Admin` (requires backfill)
- `DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN` → `Admin`
- `ADD CONSTRAINT` (validation) → `AdminSafe`
- `ADD CONSTRAINT` (FK with CASCADE DELETE) → `Admin`
- `DROP CONSTRAINT`, `DROP INDEX` → `Admin`
- `DROP TABLE`, `TRUNCATE TABLE`, `DROP DATABASE` → `AdminDestructive`

**ALTER TABLE Safety Rules**:
- Safe operations: `ADD COLUMN` (nullable), `CREATE INDEX`, validation constraints
- Risky operations: `DROP COLUMN` (data loss), `RENAME COLUMN` (app breakage), `ALTER COLUMN` (type change)
- Destructive operations: `DROP TABLE`, `TRUNCATE TABLE`

**Driver-Specific Behavior**:
- PostgreSQL: All DDL is transactional (except `CREATE INDEX CONCURRENTLY`)
- MySQL: DDL is NOT transactional; rewrites entire table for most `ALTER TABLE` ops
- SQLite: Limited `ALTER TABLE` support (only `ADD COLUMN`, `RENAME`); `DROP COLUMN` requires table recreation

**Reference**: See `crates/dbflux_mcp_server/docs/DDL_SAFETY.md` for complete safety guide with classification matrix.

### Platform Detection

`crates/dbflux_ui/src/platform.rs` handles X11/Wayland differences:
- X11 treats `WindowKind::Floating` as transient dialogs (can cause rendering issues)
- `floating_window_kind()` returns `None` on X11, `Some(Floating)` elsewhere
- `apply_window_options()` sets min size so X11 WMs emit `WM_NORMAL_HINTS`

## Common Pitfalls

1. Forgetting `cx.notify()` after state changes
2. Blocking UI thread — use `background_executor().spawn()` for DB ops
3. Entity updates in render loops — guard with `.take()`
4. Missing feature gates on driver code
5. Creating closures per cell in tables — use row-level handlers with hit-testing instead
6. Canvas re-rendering every frame — cache scroll state and only sync on meaningful changes

## Key Files

### Binary shell (`dbflux`)

| File                                                              | Purpose                                             |
| ----------------------------------------------------------------- | --------------------------------------------------- |
| `crates/dbflux/src/main.rs`                                       | App entry point, logging, window bootstrap, IPC socket |
| `crates/dbflux/src/cli.rs`                                        | CLI arg parsing, single-instance IPC client         |

### UI layer (`dbflux_ui`)

| File                                                              | Purpose                                             |
| ----------------------------------------------------------------- | --------------------------------------------------- |
| `crates/dbflux_ui/src/app_state_entity.rs`                       | `AppStateEntity` wrapper (Deref + EventEmitter)    |
| `crates/dbflux_ui/src/ui/views/workspace/mod.rs`                  | Main layout, command dispatch                       |
| `crates/dbflux_ui/src/ui/dock/sidebar_dock.rs`                   | Collapsible, resizable sidebar                      |
| `crates/dbflux_ui/src/ui/views/sidebar/mod.rs`                   | Schema tree with lazy loading                       |
| `crates/dbflux_ui/src/ui/document/mod.rs`                        | Document system exports                             |
| `crates/dbflux_ui/src/ui/document/code/mod.rs`                   | Language-aware query and script editor               |
| `crates/dbflux_ui/src/ui/document/code/execution.rs`              | Query/script execution, dangerous-query confirmation|
| `crates/dbflux_ui/src/ui/document/code/live_output.rs`            | Live output buffer for script execution             |
| `crates/dbflux_ui/src/ui/document/data_grid_panel/mod.rs`        | Data grid with table/document view modes           |
| `crates/dbflux_ui/src/ui/document/key_value/mod.rs`               | Redis key-value document view                       |
| `crates/dbflux_ui/src/ui/document/tab_manager.rs`                | MRU tab ordering                                   |
| `crates/dbflux_ui/src/ui/document/governance.rs`                  | MCP approvals view document                         |
| `crates/dbflux_ui/src/ui/overlays/cell_editor_modal.rs`         | Modal editor for JSON/long text                    |
| `crates/dbflux_ui/src/ui/overlays/history_modal.rs`              | Recent/saved queries modal                         |
| `crates/dbflux_ui/src/ui/overlays/sql_preview_modal.rs`         | SQL/query preview modal (dual-mode)                 |
| `crates/dbflux_ui/src/ui/overlays/command_palette.rs`            | Fuzzy command palette                              |
| `crates/dbflux_ui/src/ui/overlays/login_modal.rs`                | SSO login waiting modal                            |
| `crates/dbflux_ui/src/ui/overlays/sso_wizard.rs`                 | SSO account/role discovery wizard                   |
| `crates/dbflux_ui/src/ui/components/toast.rs`                     | Toast notification system                           |
| `crates/dbflux_ui/src/ui/components/data_table/table.rs`           | Virtualized data table with column resize          |
| `crates/dbflux_ui/src/ui/components/document_tree/state.rs`         | Document tree state (cursor, search, expansion)     |
| `crates/dbflux_ui/src/ui/components/tree_nav/mod.rs`              | Reusable tree navigation (cursor, expand, select)  |
| `crates/dbflux_ui/src/ui/components/value_source_selector.rs`     | Value source dropdown (Env/Secret/Parameter/Auth)   |
| `crates/dbflux_ui/src/ui/components/multi_select.rs`              | Multi-select dropdown component                    |
| `crates/dbflux_ui/src/ui/windows/settings/form_nav.rs`             | Generic 2D grid navigation for settings forms      |
| `crates/dbflux_ui/src/ui/windows/settings/auth_profiles_section.rs` | Provider-driven auth profile CRUD UI               |
| `crates/dbflux_ui/src/ui/windows/settings/proxies.rs`               | Proxy CRUD form in Settings                        |
| `crates/dbflux_ui/src/ui/windows/settings/hooks.rs`                | Hook definitions CRUD in Settings                  |
| `crates/dbflux_ui/src/ui/windows/settings/drivers.rs`               | Per-driver settings overrides UI                   |
| `crates/dbflux_ui/src/ui/windows/settings/mcp_section.rs`           | MCP settings (clients, roles, policies, audit)      |
| `crates/dbflux_ui/src/ui/windows/settings/section_trait.rs`         | SettingsSection trait                              |
| `crates/dbflux_ui/src/ui/windows/settings/form_section.rs`          | FormSection trait for keyboard navigation          |
| `crates/dbflux_ui/src/ui/windows/connection_manager/hooks_tab.rs`  | Per-profile hook bindings                           |
| `crates/dbflux_ui/src/ui/windows/connection_manager/access_tab.rs` | Unified access editor (Direct/SSH/Proxy/SSM)        |
| `crates/dbflux_ui/src/keymap/defaults.rs`                        | Key bindings per context                          |
| `crates/dbflux_ui/src/keymap/command.rs`                          | Command enum and dispatch                         |
| `crates/dbflux_ui/src/keymap/focus.rs`                            | FocusTarget (Document/Sidebar/BackgroundTasks)    |
| `crates/dbflux_ui/src/ipc_server.rs`                              | App-control IPC server (Focus, OpenScript)         |
| `crates/dbflux_ui/src/assets.rs`                                   | GPUI AssetSource impl for embedded SVG icons      |
| `crates/dbflux_ui/src/platform.rs`                                | X11/Wayland detection, window options            |

### Runtime (`dbflux_app`)

| File                                                              | Purpose                                             |
| ----------------------------------------------------------------- | --------------------------------------------------- |
| `crates/dbflux_app/src/app_state.rs`                             | AppState (plain struct, no GPUI)                   |
| `crates/dbflux_app/src/access_manager.rs`                        | AppAccessManager for direct/managed access         |
| `crates/dbflux_app/src/auth_provider_registry.rs`               | Runtime auth provider registry                      |
| `crates/dbflux_app/src/hook_executor.rs`                          | Composite hook executor routing                     |
| `crates/dbflux_app/src/proxy.rs`                                  | `create_proxy_tunnel` callback for `CreateTunnelFn` |
| `crates/dbflux_app/src/config_loader.rs`                          | SQLite-backed configuration persistence             |
| `crates/dbflux_app/src/history_manager_sqlite.rs`                 | SQLite-backed query history                        |
| `crates/dbflux_app/src/mcp_command.rs`                            | MCP subcommand integration and arg parsing         |
| `crates/dbflux_app/src/keymap/command.rs`                         | Command enum (pure domain)                         |
| `crates/dbflux_app/src/keymap/focus.rs`                           | FocusTarget enum (pure domain)                     |

### Core and supporting crates

| File                                                              | Purpose                                             |
| ----------------------------------------------------------------- | --------------------------------------------------- |
| `crates/dbflux_core/src/core/traits.rs`                           | `DbDriver`, `Connection` traits                     |
| `crates/dbflux_core/src/driver/capabilities.rs`                   | DatabaseCategory, QueryLanguage, DriverCapabilities |
| `crates/dbflux_core/src/config/app.rs`                            | Legacy config.json import (deprecated)              |
| `crates/dbflux_core/src/access/mod.rs`                            | AccessKind + AccessManager contracts               |
| `crates/dbflux_core/src/auth/mod.rs`                              | Auth provider contracts                            |
| `crates/dbflux_core/src/auth/types.rs`                            | Auth profile/session types + migration             |
| `crates/dbflux_core/src/core/error_formatter.rs`                  | ErrorFormatter trait for driver errors             |
| `crates/dbflux_core/src/query/generator.rs`                       | QueryGenerator trait, mutation/read templates       |
| `crates/dbflux_core/src/query/column_ref.rs`                     | ColumnRef type for WHERE clause column references  |
| `crates/dbflux_core/src/query/classify.rs`                        | DDL classification (AdminSafe/Admin/AdminDestructive)|
| `crates/dbflux_core/src/connection/hook.rs`                       | Hook types, HookRunner, phase orchestration        |
| `crates/dbflux_core/src/query/language_service.rs`               | Dangerous query detection (SQL, MongoDB, Redis)   |
| `crates/dbflux_core/src/pipeline/mod.rs`                          | Provider-agnostic connect pipeline orchestration   |
| `crates/dbflux_core/src/pipeline/resolve.rs`                      | ValueRef patching into DbConfig and managed access |
| `crates/dbflux_core/src/values/resolver.rs`                       | Composite secret/parameter/auth value resolver      |
| `crates/dbflux_core/src/schema/types.rs`                          | Schema types with lazy loading support             |
| `crates/dbflux_core/src/data/crud.rs`                             | CRUD mutation types for all database paradigms     |
| `crates/dbflux_core/src/data/key_value.rs`                        | Key-value operation types (Hash, Set, List, ZSet)  |
| `crates/dbflux_core/src/sql/dialect.rs`                           | SqlDialect trait for SQL flavor differences        |
| `crates/dbflux_core/src/storage/session.rs`                       | Session persistence (scratch/shadow files, manifest)|
| `crates/dbflux_core/src/config/scripts_directory.rs`              | Scripts folder tree (file/folder CRUD)             |
| `crates/dbflux_core/src/connection/context.rs`                     | Per-tab execution context (connection/database)     |
| `crates/dbflux_core/src/observability/types.rs`                   | EventRecord, EventCategory, EventSeverity, enums  |
| `crates/dbflux_core/src/observability/actions.rs`                 | Canonical action string constants                  |
| `crates/dbflux_lua/src/executor.rs`                               | Lua hook executor                                  |
| `crates/dbflux_lua/src/engine.rs`                                 | Lua VM creation and sandbox setup                  |
| `crates/dbflux_lua/src/api/dbflux.rs`                             | Lua logging, env, and process APIs                |
| `crates/dbflux_lua/src/api/connection.rs`                        | Lua connection.* API (exposes HookContext)         |
| `crates/dbflux_lua/src/api/hook.rs`                              | Lua hook.* API (phase, failure policy)             |
| `crates/dbflux_driver_mongodb/src/driver.rs`                      | MongoDB driver implementation                      |
| `crates/dbflux_driver_mongodb/src/query_parser.rs`                | MongoDB query syntax parser                        |
| `crates/dbflux_driver_mongodb/src/query_generator.rs`             | MongoDB shell query generator                      |
| `crates/dbflux_driver_redis/src/driver.rs`                        | Redis driver implementation                        |
| `crates/dbflux_driver_redis/src/command_generator.rs`            | Redis command generator                           |
| `crates/dbflux_driver_dynamodb/src/driver.rs`                     | DynamoDB driver implementation                    |
| `crates/dbflux_driver_dynamodb/src/query_parser.rs`               | DynamoDB command envelope parser                   |
| `crates/dbflux_driver_dynamodb/src/query_generator.rs`            | DynamoDB mutation envelope generator                |
| `crates/dbflux_aws/src/auth.rs`                                   | AWS auth providers + SSO login flow                |
| `crates/dbflux_aws/src/config.rs`                                 | AWS config parser/cache + profile write-back       |
| `crates/dbflux_aws/src/accounts.rs`                               | AWS SSO account/role discovery                    |
| `crates/dbflux_ipc/src/driver_protocol.rs`                        | Driver RPC protocol schema and DTOs                |
| `crates/dbflux_ipc/src/auth.rs`                                   | IPC auth token management                         |
| `crates/dbflux_driver_ipc/src/driver.rs`                          | IpcDriver and managed host lifecycle               |
| `crates/dbflux_driver_ipc/src/transport.rs`                       | Driver RPC client transport and handshake          |
| `crates/dbflux_tunnel_core/src/lib.rs`                            | Tunnel, TunnelConnector, ForwardingConnection      |
| `crates/dbflux_proxy/src/lib.rs`                                   | SOCKS5/HTTP CONNECT proxy tunnel                  |
| `crates/dbflux_driver_host/src/main.rs`                           | External RPC host server entrypoint                |
| `crates/dbflux_mcp/src/runtime.rs`                                 | MCP runtime with governance integration            |
| `crates/dbflux_mcp/src/governance_service.rs`                      | McpGovernanceService trait and DTOs               |
| `crates/dbflux_mcp/src/tool_catalog.rs`                            | Canonical MCP tools and deferred tool definitions  |
| `crates/dbflux_mcp_server/src/lib.rs`                             | MCP server library (called by `dbflux mcp`)       |
| `crates/dbflux_policy/src/engine.rs`                             | PolicyEngine, PolicyRole, ToolPolicy              |
| `crates/dbflux_policy/src/classification.rs`                       | ExecutionClassification enum                       |
| `crates/dbflux_policy/src/trusted_clients.rs`                      | TrustedClientRegistry                             |
| `crates/dbflux_approval/src/service.rs`                           | ApprovalService for pending executions             |
| `crates/dbflux_audit/src/lib.rs`                                   | AuditService: validate, preprocess, record events |
| `crates/dbflux_audit/src/query.rs`                                 | AuditQueryFilter for querying audit events         |
| `crates/dbflux_audit/src/export.rs`                                | CSV/JSON export (basic and extended schemas)      |
| `crates/dbflux_audit/src/redaction.rs`                             | Sensitive value redaction logic                   |
| `crates/dbflux_audit/src/purge.rs`                                 | Retention-based event purge (batched)             |
| `crates/dbflux_audit/src/store/sqlite.rs`                          | SQLite store adapter wrapping AuditRepository     |
| `crates/dbflux_storage/src/bootstrap.rs`                           | StorageRuntime with single dbflux.db connection   |
| `crates/dbflux_storage/src/paths.rs`                               | dbflux_db_path() returns ~/.local/share/dbflux/dbflux.db |
| `crates/dbflux_storage/src/migrations/mod.rs`                      | MigrationRegistry, Migration trait                |
| `crates/dbflux_storage/src/repositories/traits.rs`                  | Repository trait (all(), find_by_id(), upsert(), delete()) |
| `crates/dbflux_storage/src/repositories/audit.rs`                   | AuditRepository with AuditEventDto                |
| `crates/dbflux_storage/src/legacy.rs`                              | JSON-to-SQLite import (profiles, auth, ssh, config) |
