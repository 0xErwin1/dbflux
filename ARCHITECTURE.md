# Architecture

## Overview

- DBFlux is a keyboard-first database client built with Rust and GPUI, focused on fast workflows and a clean desktop UI (README.md).
- The repo is a Rust workspace with a UI app crate plus shared core types, driver implementations, and supporting libraries (Cargo.toml, crates/).
- Supports multiple database paradigms: relational (SQL), document (MongoDB, DynamoDB), key-value, graph, time-series, and wide-column stores.

## Tech Stack

- Language: Rust 2024 edition (crates/dbflux/Cargo.toml).
- UI: `gpui`, `gpui-component` (Cargo.toml).
- Databases: `tokio-postgres` (PostgreSQL), `rusqlite` (SQLite), `mysql` (MySQL/MariaDB), `mongodb` (MongoDB), `redis` (Redis), `aws-sdk-dynamodb` (DynamoDB) (Cargo.toml).
- AWS auth/integration: `aws-config`, `aws-sdk-sso`, `aws-sdk-ssooidc`, `aws-sdk-sts`, `aws-sdk-secretsmanager`, `aws-sdk-ssm` (`dbflux_aws`).
- IPC/RPC: `interprocess` local sockets + `bincode` message framing (`dbflux_ipc`, `dbflux_driver_ipc`, `dbflux_driver_host`).
- SSH: `ssh2` via `dbflux_ssh` (crates/dbflux_ssh/src/lib.rs).
- Export: `csv` + `hex` + `base64` + `serde_json` via `dbflux_export` (crates/dbflux_export/src/lib.rs).
- Serialization/config: `serde`, `serde_json`, `dirs` (Cargo.toml).
- Logging: `log`, `env_logger` (crates/dbflux/src/main.rs).

## Directory Structure

```
crates/
  dbflux/                   # Binary shell: main entry point, CLI, single-instance IPC
    src/
      main.rs               # Application entry point, logging, window bootstrap, IPC socket
      cli.rs                # CLI arg parsing, single-instance IPC client
  dbflux_ui/                # GPUI UI layer: views, documents, overlays, components, keymap
    src/
      app_state_entity.rs   # AppStateEntity wrapper (Deref + EventEmitter)
      ui/                   # UI panels, windows, theme
        mod.rs
        theme.rs            # Theme definitions
        tokens.rs           # Design tokens (spacing, sizing constants)
        icons/              # SVG icon system (AppIcon enum)
        dock/               # Resizable dock panels
          mod.rs            # Bottom dock support
          sidebar_dock.rs   # Collapsible, resizable sidebar
        views/              # Primary views (workspace and sidebar)
          mod.rs
          status_bar.rs     # Status bar rendering
          tasks_panel.rs    # Background tasks panel
          workspace/        # Main layout, command dispatch, focus routing
            mod.rs
            actions.rs      # Workspace-level action handlers
            dispatch.rs     # Command dispatch logic
            render.rs       # Workspace rendering
          sidebar/          # Connection + scripts tree with folders, drag-drop
            mod.rs
            code_generation.rs
            context_menu.rs
            deletion.rs
            drag_drop.rs
            expansion.rs
            operations.rs
            render.rs
            render_footer.rs
            render_overlays.rs
            render_tree.rs
            selection.rs
            table_loading.rs
            tree_builder.rs
        overlays/            # Modals and transient overlays
          mod.rs
          cell_editor_modal.rs     # Modal editor for JSON/long text cells
          command_palette.rs       # Fuzzy command palette
          document_preview_modal.rs # JSON document preview modal
          history_modal.rs         # Recent/saved queries modal
          shutdown_overlay.rs      # Graceful shutdown overlay
          sql_preview_modal.rs     # SQL/query preview (dual-mode: SQL and generic)
          login_modal.rs           # SSO login waiting modal with timeout
          sso_wizard.rs            # SSO account/role discovery wizard
        document/            # Document-based tab system (like VS Code/DBeaver)
          mod.rs             # Document exports and shared types
          handle.rs          # DocumentHandle for entity management
          types.rs           # DocumentId, DocumentKind, DocumentState
          result_view.rs     # ResultView enum (Table, LiveOutput, etc.)
          task_runner.rs     # Background task tracking for documents
          data_document.rs   # Standalone data browsing document
          tab_manager.rs     # MRU tab ordering and activation
          tab_bar.rs         # Visual tab bar rendering
          data_view.rs       # DataViewMode abstraction (Table vs Document)
          add_member_modal.rs # Modal for adding Redis set/list/sorted-set members
          new_key_modal.rs   # Modal for creating new Redis keys
          governance.rs      # MCP approvals view for pending executions
          code/              # CodeDocument: query/script editor
            mod.rs
            completion.rs    # Language-aware autocompletion
            context_bar.rs   # Execution context dropdowns (connection/database/schema)
            diagnostics.rs   # Live query diagnostics
            execution.rs     # Query and script execution flow (incl. dangerous-query confirmation)
            file_ops.rs      # Auto-save, scratch/shadow file management
            focus.rs         # Internal focus management
            live_output.rs   # Document-owned streamed script output buffer
            render.rs        # Toolbar, editor, and live output rendering
          data_grid_panel/   # Data grid with table/document view modes
            mod.rs
            context_menu.rs
            mutations.rs
            navigation.rs
            query.rs
            render.rs
            utils.rs
          key_value/         # Redis/key-value-specific document view
            mod.rs
            commands.rs
            context_menu.rs
            copy_command.rs
            document_view.rs
            mutations.rs
            pagination.rs
            parsing.rs
            render.rs
        components/          # Reusable UI components
          mod.rs
          context_menu.rs    # Reusable context menu component
          dropdown.rs        # Reusable dropdown selector
          form_navigation.rs # FormNavigation / FormEditState traits
          form_renderer.rs   # Generic form field rendering
          json_editor_view.rs # Inline JSON editor component
          modal_frame.rs     # Reusable modal chrome/frame
          toast.rs           # Custom toast notification system
          multi_select.rs    # Multi-select dropdown component
          value_source_selector.rs # Value source dropdown (Env/Secret/Parameter/Auth)
          data_table/        # Custom virtualized data table
            mod.rs
            table.rs         # Main table component with phantom scroller
            state.rs         # Table state management
            model.rs         # CellValue and data model
            selection.rs      # Selection handling
            events.rs         # Event handling
            clipboard.rs      # Copy/paste support
            theme.rs         # Table styling
          document_tree/     # Hierarchical document/JSON viewer
            mod.rs
            state.rs         # Tree state with cursor, expansion, search
            tree.rs          # Tree rendering with keyboard navigation
            node.rs          # Node types (document, field, array item)
            events.rs        # Document tree events (selection, context menu)
          tree_nav/          # Reusable tree navigation component
            mod.rs
            gutter.rs
        windows/             # Settings and connection manager windows
          mod.rs
          ssh_shared.rs      # Shared SSH auth UI components
          settings/          # Settings window sections
            mod.rs
            render.rs        # Top-level settings window rendering
            lifecycle.rs     # Settings window open/close/save logic
            sidebar_nav.rs   # Settings sidebar navigation (TreeNav)
            dirty_state.rs   # Unsaved-changes tracking for settings forms
            form_nav.rs      # FormGridNav<F> generic 2D grid navigation
            form_section.rs  # FormSection trait for keyboard navigation
            section_trait.rs # SettingsSection trait
            general.rs       # General settings (theme, safety toggles)
            keybindings.rs   # Keybindings settings section
            auth_profiles_section.rs # Dynamic auth profile CRUD by provider form definition
            proxies.rs       # Proxy CRUD form with FormGridNav
            ssh_tunnels.rs   # SSH tunnel CRUD form with FormGridNav
            hooks.rs         # Hook definitions CRUD
            drivers.rs       # Per-driver settings overrides
            rpc_services.rs  # External RPC service management
            mcp_section.rs   # MCP settings (trusted clients, roles, policies, audit)
          connection_manager/ # Connection manager window
            mod.rs
            access_tab.rs    # Unified access mode editor (Direct/SSH/Proxy/SSM)
            form.rs          # Connection form state and field management
            navigation.rs    # Keyboard navigation within connection manager
            render.rs        # Top-level connection manager rendering
            render_driver_select.rs
            render_tabs.rs
            hooks_tab.rs     # Per-profile hook bindings
      keymap/                # Keyboard system
        defaults.rs          # Context-aware key bindings
        command.rs           # Command enum and dispatch
        focus.rs             # FocusTarget (Document/Sidebar/BackgroundTasks)
      ipc_server.rs          # App-control IPC server (Focus, OpenScript)
      assets.rs              # GPUI AssetSource impl for embedded SVG icons
      platform.rs            # X11/Wayland detection, window options
  dbflux_app/               # Runtime/domain: AppState (plain struct), managers, hooks, auth
    src/
      app_state.rs          # AppState (plain struct, no GPUI dependency)
      access_manager.rs      # AppAccessManager for direct/managed access
      auth_provider_registry.rs # Runtime auth provider registry
      hook_executor.rs       # Composite hook executor routing
      proxy.rs               # create_proxy_tunnel callback for CreateTunnelFn
      config_loader.rs       # SQLite-backed configuration persistence
      history_manager_sqlite.rs # SQLite-backed query history
      mcp_command.rs         # MCP subcommand integration and arg parsing
      keymap/                # Keyboard system (pure domain types)
        command.rs           # Command enum (pure domain)
        focus.rs             # FocusTarget enum (pure domain)
  dbflux_core/              # Traits, core types, storage, errors
    src/access/             # AccessKind, AccessManager, and managed-access serialization
      mod.rs
    src/auth/               # AuthProfile + DynAuthProvider contracts
      mod.rs
      types.rs
    src/core/               # Fundamental types and traits
      traits.rs             # DbDriver + Connection traits
      error.rs              # DbError type
      error_formatter.rs    # ErrorFormatter trait for driver-specific error messages
      value.rs              # Generic Value type for cross-database data
      shutdown.rs           # ShutdownCoordinator
      task.rs               # Background task tracking
    src/driver/             # Driver metadata and form definitions
      capabilities.rs       # DatabaseCategory, QueryLanguage, DriverCapabilities, DriverMetadata
      form.rs               # Dynamic form definitions per driver
    src/schema/             # Database schema types
      types.rs              # Schema types (tables, collections, indexes, FKs)
      builder.rs            # Builder helpers for schema construction
      node_id.rs            # SchemaNodeId for tree identification
    src/sql/                # SQL generation and dialects
      dialect.rs            # SqlDialect trait for SQL flavor differences
      generation.rs         # SQL INSERT/UPDATE/DELETE generation
      query_builder.rs      # SqlQueryBuilder for safe query construction
      code_generation.rs    # DDL code generation (indexes, types, FKs)
    src/query/              # Query types and language services
      types.rs              # QueryRequest, QueryResult, Row, ColumnMeta
      generator.rs          # QueryGenerator trait, mutation/read templates, semantic preview helpers
      language_service.rs   # Dangerous query detection (SQL, MongoDB, Redis)
      safety.rs             # Safe read query detection
      table_browser.rs      # Table browsing state and pagination
    src/connection/         # Connection management and profiles
      profile.rs            # Connection/SSH profiles
      profile_manager.rs    # ProfileManager
      manager.rs            # ConnectionManager, schema caching, connect flow
      hook.rs               # Hook definitions, HookRunner, phase orchestration
      tree.rs               # Folder/connection tree model
      tree_store.rs         # Tree persistence (JSON)
      tree_manager.rs       # ConnectionTreeManager
      context.rs            # Per-tab execution context (connection/database/schema)
      proxy.rs              # ProxyProfile, ProxyKind, ProxyAuth, no_proxy matching
      proxy_manager.rs      # ProxyManager (type alias for ItemManager<ProxyProfile>)
      ssh_tunnel_manager.rs # SshTunnelManager
      item_manager.rs       # Generic ItemManager<T>, Identifiable, DefaultFilename traits
    src/storage/            # Persistence and state
      json_store.rs         # JsonStore<T> generic with type aliases (profiles, tunnels, proxies)
      session.rs            # Session persistence (scratch/shadow files, manifest)
      history.rs            # History persistence
      history_manager.rs    # HistoryManager
      saved_query.rs        # Saved queries persistence
      saved_query_manager.rs # SavedQueryManager
      recent_files.rs       # Recent files tracking
      secrets.rs            # Keyring secret storage
      secret_manager.rs     # SecretManager with HasSecretRef trait
      ui_state.rs           # UiStateStore for persisted UI state (sidebar collapse)
    src/data/               # Data types and operations
      crud.rs               # CRUD mutation types for all database paradigms
      key_value.rs          # Key-value operation types (Hash, Set, List, ZSet, Stream)
      view.rs               # DataViewMode (Table/Document) abstraction
    src/config/             # Application configuration
      app.rs                # Legacy config.json import (deprecated)
      refresh_policy.rs     # Schema refresh policy
      scripts_directory.rs  # Scripts folder tree (file/folder CRUD)
    src/pipeline/           # Pre-connect pipeline (auth/value/access stages)
      mod.rs
      resolve.rs
    src/values/             # ValueRef resolution + provider registry + cache
      resolver.rs
    src/facade/             # Session facade
      session.rs            # Session facade for connection management
  dbflux_ipc/               # Versioned IPC contracts and framing
    src/auth.rs             # IPC auth token generation and file storage
    src/envelope.rs         # ProtocolVersion + app/driver protocol constants
    src/protocol.rs         # Single-instance app-control messages
    src/driver_protocol.rs  # Driver RPC request/response schema (DTOs + errors)
    src/framing.rs          # Length-prefixed bincode transport framing
    src/socket.rs           # Cross-platform socket naming helpers
  dbflux_driver_ipc/        # DbDriver adapter for external RPC services
    src/driver.rs           # IpcDriver + managed host lifecycle
    src/transport.rs        # RPC client transport and handshake
    src/connection.rs       # Connection proxy over driver RPC
  dbflux_driver_host/       # Host process that serves drivers over RPC
    src/main.rs             # Driver RPC server entry point
    src/session.rs          # Session manager and method dispatch
  dbflux_driver_postgres/   # PostgreSQL driver implementation
  dbflux_driver_sqlite/     # SQLite driver implementation
  dbflux_driver_mysql/      # MySQL/MariaDB driver implementation
  dbflux_driver_mongodb/    # MongoDB driver implementation
    src/driver.rs           # Connection, schema discovery, CRUD operations
    src/query_parser.rs     # MongoDB query syntax parser (db.collection.method())
    src/query_generator.rs  # MongoDB shell query generator (insertOne, updateOne, etc.)
  dbflux_driver_redis/      # Redis driver implementation
    src/driver.rs           # Connection, key-value API, schema discovery
    src/command_generator.rs # Redis command generator (SET, HSET, SADD, etc.)
  dbflux_driver_dynamodb/   # DynamoDB driver implementation
    src/driver.rs           # Connection, schema discovery, scan/query/put/update/delete
    src/query_parser.rs     # JSON command envelope parser for DynamoDB operations
    src/query_generator.rs  # Mutation -> DynamoDB command envelope generator
    tests/live_integration.rs # Docker-backed integration tests (DynamoDB Local)
  dbflux_aws/               # AWS auth providers + Secrets Manager/SSM value providers
    src/auth.rs             # AWS SSO/shared/static providers and SSO login flow
    src/config.rs           # ~/.aws/config parser/cache and profile write-back helpers
    src/accounts.rs         # AWS SSO account and role discovery
  dbflux_ssm/               # AWS SSM tunnel factory for managed access
  dbflux_lua/               # Embedded Lua runtime for in-process hooks
    src/executor.rs         # Lua HookExecutor implementation
    src/engine.rs           # Lua VM creation and shared runtime state
    src/api/dbflux.rs       # dbflux.log/env/process Lua APIs
    src/api/connection.rs   # Lua connection.* API (exposes HookContext)
    src/api/hook.rs         # Lua hook.* API (phase, failure policy)
  dbflux_tunnel_core/       # Shared RAII tunnel infrastructure
    src/lib.rs              # Tunnel, TunnelConnector, ForwardingConnection<R>
  dbflux_proxy/             # SOCKS5/HTTP CONNECT proxy tunnel
    src/lib.rs              # ProxyTunnelConfig, SOCKS5/HTTP handshake, tunnel loop
  dbflux_ssh/               # SSH tunnel support
  dbflux_export/            # Export (CSV, JSON, Text, Binary)
    src/lib.rs              # Shape-based export API and format dispatch
    src/binary.rs           # Binary/hex/base64 exporter
    src/csv.rs              # CSV exporter
    src/json.rs             # JSON pretty/compact exporter
    src/text.rs             # Text table exporter
  dbflux_mcp/               # MCP runtime and governance
    src/lib.rs              # Exports for runtime, governance service, tool catalog
    src/runtime.rs          # McpRuntime implementing McpGovernanceService
    src/governance_service.rs # McpGovernanceService trait and DTOs
    src/tool_catalog.rs     # Canonical MCP tools and deferred tool definitions
    src/built_ins.rs        # Built-in roles and policies
    src/handlers/           # MCP tool handlers (query, approval, audit, discovery, scripts)
    src/server/             # MCP server infrastructure (router, authorization, bootstrap)
  dbflux_mcp_server/        # Standalone MCP server binary
    src/main.rs             # CLI entrypoint with --client-id and --config-dir
    src/server.rs           # JSON-RPC request loop over stdin/stdout
    src/bootstrap.rs        # Runtime initialization and state
    src/transport.rs        # Line-based stdin/stdout transport
    src/connection_cache.rs # Connection pool for the standalone server
    src/handlers/           # Tool handlers adapted for standalone operation
  dbflux_policy/            # Policy engine and classification
    src/lib.rs              # Exports for engine, classification, trusted clients
    src/classification.rs   # ExecutionClassification enum (Metadata/Read/Write/Destructive/AdminSafe/Admin/AdminDestructive)
    src/engine.rs           # PolicyEngine with PolicyRole and ToolPolicy
    src/trusted_clients.rs  # TrustedClientRegistry for known AI clients
    src/assignments.rs      # ConnectionPolicyAssignment and PolicyBindingScope
  dbflux_approval/           # Approval service for deferred executions
    src/lib.rs              # Exports for ApprovalService and pending store
    src/service.rs          # ApprovalService (approve/reject lifecycle)
    src/store.rs            # InMemoryPendingExecutionStore and ExecutionPlan
  dbflux_audit/             # Audit logging
    src/lib.rs              # AuditService: validate, fingerprint, redact, record
    src/query.rs            # AuditQueryFilter (actor, category, action, outcome, date range)
    src/export.rs           # Audit export to JSON/CSV (basic and extended schemas)
    src/redaction.rs        # Sensitive value redaction for details_json and error_message
    src/purge.rs            # Retention-based event purge (batched deletes)
    src/store/sqlite.rs     # SqliteAuditStore delegating to AuditRepository
  dbflux_storage/            # Unified SQLite storage
    src/bootstrap.rs        # StorageRuntime with single dbflux.db connection
    src/paths.rs            # dbflux_db_path() returns ~/.local/share/dbflux/dbflux.db
    src/migrations/         # Trait-based migration system
      mod.rs                # MigrationRegistry, Migration trait
      *.rs                  # Individual migration files (001_initial.rs, etc.)
    src/repositories/       # All domain repositories
      traits.rs             # Repository trait (all(), find_by_id(), upsert(), delete())
      audit.rs              # AuditRepository with AuditEventDto
      *.rs                  # Other domain repositories
    src/legacy.rs           # JSON-to-SQLite import
  dbflux_test_support/       # Docker containers and fixtures for integration tests
    src/containers.rs       # Docker container lifecycle (Postgres, MySQL, MongoDB, Redis, DynamoDB Local)
    src/fixtures.rs         # Test fixture helpers
    src/fake_driver.rs      # FakeDriver for unit tests
```

## Core Components

### Application Layer

- App entry point: `crates/dbflux/src/main.rs` initializes logging, theme, and main GPUI window.
- Global app state: `crates/dbflux_app/src/app_state.rs` (plain struct, no GPUI dependency) holds drivers, profiles, active connections, history, task manager, and secret store access.
- CLI and single-instance: `crates/dbflux/src/cli.rs` parses arguments; `crates/dbflux_ui/src/ipc_server.rs` runs the app-control IPC server for `Focus` and `OpenScript` commands.
- Assets: `crates/dbflux_ui/src/assets.rs` implements GPUI's `AssetSource` to serve embedded SVG icons.
- Workspace UI shell: `crates/dbflux_ui/src/ui/views/workspace/` wires panes (sidebar/dock, document area, bottom dock), command palette, and focus routing. Split across `mod.rs`, `actions.rs`, `dispatch.rs`, and `render.rs`.

### Document System

`crates/dbflux_ui/src/ui/document/` implements a tab-based document architecture:

- `DocumentHandle` manages document lifecycle as GPUI entities
- `CodeDocument` provides language-aware editing for queries and scripts (SQL/MongoDB/Redis/Lua/Python/Bash) with multiple result tabs and live output for scripts. Connection/database/schema controls are only shown for languages that support connection context.
- Auto-save: tabs auto-save to scratch files (untitled) or shadow files (file-backed) on a 2-second debounce. Explicit Ctrl+S writes to the original file. Tabs close without warnings.
- Session restore: `SessionStore` persists a manifest of open tabs to `~/.local/share/dbflux/sessions/`. On startup, all tabs are restored with conflict detection for externally modified files.
- `DataDocument` enables standalone data browsing independent of queries
- `TabManager` tracks MRU (Most Recently Used) order for tab switching
- `DataGridPanel` renders data with switchable view modes (Table for SQL, Document tree for MongoDB)
- Duplicate prevention: opening an already-open table/collection focuses the existing tab

### Data Visualization

- **Data table**: `crates/dbflux_ui/src/ui/components/data_table/` custom virtualized table with sorting, selection, horizontal scrolling via phantom scroller pattern, keyboard navigation, column resizing, and context menu with CRUD operations.
- **Document tree**: `crates/dbflux_ui/src/ui/components/document_tree/` hierarchical JSON/BSON viewer for document databases with keyboard navigation (j/k/h/l), search (Ctrl+F or /), collapsible nodes, and view modes (Keys Only, Keys+Preview, Full Values).
- **Key-value view**: `crates/dbflux_ui/src/ui/document/key_value/` Redis-specific document view with per-type rendering (String, Hash, List, Set, SortedSet, Stream), pagination, mutations, and context menu.
- Cell editor modal: `crates/dbflux_ui/src/ui/overlays/cell_editor_modal.rs` provides a modal editor for JSON columns and long/multiline text, with JSON validation and formatting.
- Document preview modal: `crates/dbflux_ui/src/ui/overlays/document_preview_modal.rs` full-screen JSON document preview with an inline JSON editor.
- Command palette: `crates/dbflux_ui/src/ui/overlays/command_palette.rs` fuzzy-search command palette for all app actions.

### Schema & Navigation

- Sidebar: `crates/dbflux_ui/src/ui/views/sidebar/` displays two tabs — Connections (schema tree with folder organization, drag-drop, multi-selection) and Scripts (file/folder management for saved query files, script hooks, and other user files). Switch tabs with `q` or `e` keys. Shows tables/collections, columns, indexes per database category with lazy loading.
- Sidebar dock: `crates/dbflux_ui/src/ui/dock/sidebar_dock.rs` provides collapsible, resizable sidebar with ToggleSidebar command (Ctrl+B).
- Connection tree: `crates/dbflux_core/src/connection/tree.rs` models folders and connections as a tree structure with persistence via `connection_tree_store.rs`.

### Driver System

- **Driver capabilities**: `crates/dbflux_core/src/driver/capabilities.rs` defines:
  - `DatabaseCategory`: Relational, Document, KeyValue, Graph, TimeSeries, WideColumn
  - `QueryLanguage`: SQL, MongoQuery, RedisCommands, Cypher, InfluxQuery, CQL (with editor mode, placeholder, comment prefix)
  - `DriverCapabilities`: bitflags for features like PAGINATION, TRANSACTIONS, NESTED_DOCUMENTS, etc.
  - `DriverMetadata`: static driver info (id, name, category, query_language, capabilities, icon)
- **Error formatting**: `crates/dbflux_core/src/core/error_formatter.rs` provides `ErrorFormatter` trait for driver-specific error messages with context (detail, hint, column, table, constraint).
- Core domain API: `crates/dbflux_core/src/core/traits.rs` defines `DbDriver`, `Connection`, SQL generation, and cancellation contracts.
- **Query generation**: `crates/dbflux_core/src/query/generator.rs` defines `QueryGenerator` as the driver-owned source of truth for mutation text plus read/query templates. SQL drivers use `SqlMutationGenerator`; MongoDB, Redis, and DynamoDB expose their own native generators. The UI and MCP access generators through `Connection::query_generator()` so previews and copied queries come from the driver rather than a UI-local formatter.
- Driver forms: `crates/dbflux_core/src/driver/form.rs` defines dynamic form schemas that drivers provide for connection configuration. Supports both form-based and URI connection modes.
- **Driver/UI decoupling**: The UI never checks driver IDs directly. Instead, it uses `DriverMetadata` abstractions (`DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`) to adapt behavior. This allows new drivers to work automatically without UI changes.

### Auth & Access Pipeline

- `crates/dbflux_app/src/auth_provider_registry.rs` maintains runtime `DynAuthProvider` registration in the app crate and avoids hardcoding AWS provider logic in connection UI flows.
- `crates/dbflux_core/src/auth/` defines provider contracts (`AuthFormDef`, `DynAuthProvider`, `ImportableProfile`, `after_profile_saved`) and serializable auth profile/session types.
- `AuthProfile` storage migrated from provider-specific nested `config` payloads to provider-agnostic `fields`, with compatibility deserialization for legacy entries.
- `crates/dbflux_core/src/access/mod.rs` introduces provider-agnostic `AccessKind::Managed { provider, params }` with transparent migration from legacy `method = "ssm"` profile JSON.
- `crates/dbflux_core/src/pipeline/mod.rs` runs pre-connect stages (`Authenticating` -> `ResolvingValues` -> `OpeningAccess`) and publishes `PipelineState` updates to UI watchers.
- `crates/dbflux_app/src/access_manager.rs` provides the app-side `AccessManager` implementation for direct and managed access providers (currently `aws-ssm`).

### Tunnel Infrastructure

- `crates/dbflux_tunnel_core/` provides a shared RAII `Tunnel` struct that binds a local port, verifies connectivity, and spawns a background forwarding thread that shuts down on drop.
- `TunnelConnector` trait: implementations provide `test_connection()` and `run_tunnel_loop()` for protocol-specific forwarding (SOCKS5, HTTP CONNECT, SSH).
- `ForwardingConnection<R>`: bidirectional forwarding between a local `TcpStream` and a generic remote `R` (`TcpStream` for proxy, `ssh2::Channel` for SSH). Write strategies are injected via function pointers.
- `adaptive_sleep()`: 50ms when idle, 1ms when connections exist, skip when data was transferred.
- `crates/dbflux_proxy/`: SOCKS5 and HTTP CONNECT proxy tunnel via `TunnelConnector` impl.
- `crates/dbflux_ssh/`: SSH tunnel via `TunnelConnector` impl. All SSH operations serialized to a single thread for libssh2 safety.
- Proxy+SSH are mutually exclusive per connection (enforced in `ConnectProfileParams::execute()`).
- `CreateTunnelFn` callback in `dbflux_core` avoids circular dependency: the app crate supplies the real proxy implementation.

### Connection Hooks

- `crates/dbflux_core/src/connection/hook.rs` defines reusable hook definitions with three execution modes: `Command`, `Script`, and `Lua`.
- Process-backed hooks can be inline or file-backed and cover Bash/Python plus arbitrary commands.
- Lua hooks run in-process through `dbflux_lua`, with capability-gated access to `hook.*`, `connection.*`, `dbflux.log.*`, `dbflux.env.*`, and `dbflux.process.run()`.
- Profile phase bindings: `PreConnect`, `PostConnect`, `PreDisconnect`, `PostDisconnect`.
- `HookRunner` orchestrates execution with `HookPhaseOutcome` (success/warning/abort).
- Process-backed hooks and Lua-triggered subprocesses share a common streaming executor. Output is visible in the Tasks panel for lifecycle hooks and in the document results panel for editor-run scripts.
- Failure policies: `Disconnect` (abort flow), `Warn` (continue with warning), `Ignore` (log only).
- Settings UI: `settings/hooks.rs` for global definitions; Connection Manager `hooks_tab.rs` for per-profile phase bindings.

### Settings Window

- Settings is organized into 8 sections: General, Keybindings, Auth Profiles, Proxies, SSH Tunnels, Services, Hooks, Drivers.
- Sidebar uses `TreeNav` component with collapsible Network/Connection categories.
- `UiStateStore` persists sidebar collapse state to `st_ui_state` table in `~/.local/share/dbflux/dbflux.db`.
- Auth Profiles section is provider-driven (`DynAuthProvider::form_def`) and supports importing provider-discovered profiles (for AWS, from `~/.aws/config`).
- Proxy and SSH tunnel forms use `FormGridNav<F>` for keyboard-driven 2D grid navigation.
- Drivers section shows per-driver settings overrides filtered by `DatabaseCategory`.

### IPC/RPC Integration

- `crates/dbflux_ipc/` defines versioned app-control and driver RPC contracts, transport framing, cross-platform socket naming, and IPC auth tokens (`auth.rs`).
- `crates/dbflux_ui/src/ipc_server.rs` runs the app-control IPC server for single-instance behavior (`Focus`, `OpenScript`). `crates/dbflux/src/cli.rs` acts as the IPC client when a second instance is launched.
- `crates/dbflux_core/src/config/app.rs` handles legacy config.json import only (deprecated).
- `crates/dbflux_app/src/app_state.rs` probes each configured RPC service at startup (`Hello`) and registers it as an in-memory driver key `rpc:<socket_id>`.
- `crates/dbflux_driver_ipc/src/driver.rs` implements `DbDriver` as an RPC proxy and only shuts down managed hosts that DBFlux spawned itself.
- External connection profiles use `DbConfig::External { kind, values }`, where form values come from the remote `form_definition` returned during `Hello`.

### SQL Generation

- **SQL dialect**: `crates/dbflux_core/src/sql/dialect.rs` defines `SqlDialect` trait for database-specific SQL syntax (quoting, LIMIT/OFFSET, type mapping).
- **SQL generation**: `crates/dbflux_core/src/sql/generation.rs` provides INSERT/UPDATE/DELETE statement generation.
- **Query builder**: `crates/dbflux_core/src/sql/query_builder.rs` offers `SqlQueryBuilder` for safe, parameterized query construction.

### CRUD Operations

- **Mutation types**: `crates/dbflux_core/src/data/crud.rs` defines `MutationRequest` enum covering all database paradigms:
  - SQL: INSERT/UPDATE/DELETE with WHERE clauses
  - Document: insertOne/updateOne/deleteOne/deleteMany
  - Key-Value: SET/DELETE/HASH_SET/SET_ADD/LIST_PUSH/ZSET_ADD and their remove counterparts, plus STREAM_ADD
- **Key-value types**: `crates/dbflux_core/src/data/key_value.rs` defines Vec-based request structs for variadic Redis commands (e.g., `HashSetRequest.fields: Vec<(String, String)>`, `SetAddRequest.members: Vec<String>`).
- **Query safety**: `crates/dbflux_core/src/query/language_service.rs` detects dangerous queries across all languages (SQL DELETE/DROP/TRUNCATE, MongoDB deleteMany/drop, Redis FLUSHALL/FLUSHDB/KEYS) and prompts for confirmation before execution.

### Storage & Configuration

**Unified SQLite storage**: All runtime data is stored in a single SQLite database at `~/.local/share/dbflux/dbflux.db`. This replaced three separate stores (config.db, state.db, audit.sqlite).

**Domain table prefixes**:
- `cfg_*` — config domain (profiles, auth, proxy, SSH, hooks, services, governance, drivers, folders)
- `st_*` — state domain (sessions, tabs, query history, saved queries, recent items, UI state, schema cache)
- `aud_*` — audit domain (audit events, entities, attributes)
- `sys_*` — system domain (migrations, metadata, legacy imports)

**Storage crate** (`dbflux_storage/`):
- `bootstrap.rs`: `StorageRuntime` manages the single `dbflux.db` connection with lazy initialization
- `paths.rs`: `dbflux_db_path()` returns the database path
- `migrations/`: Trait-based migration system (`Migration` trait with `name()` and `run(&Transaction)`). `MigrationRegistry` holds all migrations and runs them in order, tracking completion in `sys_migrations`. Idempotent — checks `sys_migrations` before running.
- `repositories/`: All domain repositories implement the `Repository` trait (`all()`, `find_by_id()`, `upsert()`, `delete()`). `AuditRepository` handles audit events with `AuditEventDto`.
- `legacy.rs`: Imports legacy JSON files into SQLite on first startup (idempotent, tracked in `sys_legacy_imports`)

**Legacy JSON import order**: Auth/proxy/SSH first, then connection profiles (FK dependency order). Import sources:
- `profiles.json` → `cfg_connection_profiles` + child tables
- `auth_profiles.json` → `cfg_auth_profiles`
- `ssh_tunnels.json` → `cfg_ssh_tunnel_profiles`
- `config.json` → `cfg_services` (RPC services only)

**Secrets**: `SecretManager` uses `HasSecretRef` trait for keyring operations. Secrets are stored in the OS keyring, references stored in SQLite.

**Session persistence**: Scratch/shadow files and session manifest in `~/.local/share/dbflux/sessions/` for tab restore on startup.

**Execution context**: `crates/dbflux_core/src/connection/context.rs` tracks per-tab connection, database, and schema selection; serialized as annotation comments in saved files.

**History modal**: `crates/dbflux_ui/src/ui/overlays/history_modal.rs` provides a unified modal for browsing recent queries and saved queries with search, favorites, and rename support.

### Driver Implementations

- **PostgreSQL**: `crates/dbflux_driver_postgres/` — `tokio-postgres` with TLS, cancellation, detailed error extraction.
- **MySQL/MariaDB**: `crates/dbflux_driver_mysql/` — dual connection architecture (sync for schema, async for queries).
- **SQLite**: `crates/dbflux_driver_sqlite/` — `rusqlite` file-based connections.
- **MongoDB**: `crates/dbflux_driver_mongodb/` — `mongodb` async driver with:
  - BSON value handling and conversion
  - Query parser for `db.collection.method()` syntax
  - Collection browsing with pagination
  - Index discovery
  - Document CRUD operations
  - Shell query generator (`MongoShellGenerator`) for insertOne/updateOne/deleteOne
- **Redis**: `crates/dbflux_driver_redis/` — `redis` driver with:
  - Key-value API for String, Hash, List, Set, SortedSet, and Stream types
  - Variadic commands (HSET with multiple fields, SADD with multiple members, etc.)
  - Keyspace (database index) support
  - Key scanning, TTL management, rename, type discovery
  - Command generator (`RedisCommandGenerator`) for all key-value mutation types
- **DynamoDB**: `crates/dbflux_driver_dynamodb/` — `aws-sdk-dynamodb` driver with:
  - Native table discovery (`ListTables`, `DescribeTable`) with PK/SK + GSI/LSI key metadata mapped to DBFlux document abstractions
  - Read path planning (`Scan` vs `Query`) with read options (`index`, `consistent_read`) and server-filter translation/fallback controls
  - Mutation support for single and multi-item paths (`put`, `update`, `delete`), with single-item upsert and bounded retry handling for unprocessed batch writes
  - JSON command-envelope parser for execute mode (`scan`, `query`, `put`, `update`, `delete`) and mutation query generation (`DynamoQueryGenerator`)
  - Current limits: no query cancellation, no PartiQL/transaction API surface, and no `update many + upsert` combination

### Driver README policy

- Each driver crate (`crates/dbflux_driver_*/`) has a `README.md` that documents current features and limitations.
- Keep those README files aligned with `DriverMetadata` capabilities and actual runtime behavior after any driver change.

### Supporting Components

- Toast system: `crates/dbflux_ui/src/ui/components/toast.rs` custom implementation with auto-dismiss (4s) for success/info/warning toasts.
- Tunnel infrastructure: `crates/dbflux_tunnel_core/` provides RAII `Tunnel` with `TunnelConnector` trait and `ForwardingConnection<R>` bidirectional forwarder.
- Proxy tunneling: `crates/dbflux_proxy/` implements SOCKS5 and HTTP CONNECT proxy tunnels via `TunnelConnector`.
- SSH tunneling: `crates/dbflux_ssh/src/lib.rs` implements SSH tunnel via `TunnelConnector`, all operations serialized to one thread for libssh2 safety.
- Export: `crates/dbflux_export/` provides shape-based export (CSV, JSON pretty/compact, Text, Binary/Hex/Base64). Format availability is determined by `QueryResultShape`, not by driver. Each format has its own module (`binary.rs`, `csv.rs`, `json.rs`, `text.rs`).
- Test support: `crates/dbflux_test_support/` provides Docker container management and fixtures for live integration tests across all drivers. DynamoDB Local is used only for integration tests and local validation; production usage targets remote AWS DynamoDB endpoints.
- Icon system: `crates/dbflux_ui/src/ui/icons/mod.rs` centralized AppIcon enum with embedded SVG assets loaded via `assets.rs`.
- Platform detection: `crates/dbflux_ui/src/platform.rs` handles X11/Wayland differences with `is_x11()`, `floating_window_kind()`, and `apply_window_options()` for proper window min size hints.

### MCP Governance System

DBFlux supports the Model Context Protocol (MCP) for AI client integration with a complete governance layer:

**Classification** (`dbflux_policy/classification.rs`):
- `ExecutionClassification` enum: Metadata, Read, Write, Destructive, AdminSafe, Admin, AdminDestructive
- Used to categorize operations by impact level for policy decisions and approval flows

**Policy Engine** (`dbflux_policy/engine.rs`):
- `PolicyEngine::evaluate()` takes actor, connection, tool, and classification
- Returns `PolicyDecision::Allow` or `PolicyDecision::Deny(reason)`
- `PolicyRole` composes multiple tool policies
- `ToolPolicy` defines allowed tools and classification levels
- `ConnectionPolicyAssignment` binds actors/connections to roles and policies

**Trusted Clients** (`dbflux_policy/trusted_clients.rs`):
- `TrustedClientRegistry` identifies known AI clients by id, name, issuer
- Used to differentiate between trusted and untrusted actors in audit logs

**Approval Flow** (`dbflux_approval`):
- `ApprovalService` manages approve/reject lifecycle for deferred executions
- `InMemoryPendingExecutionStore` holds pending executions awaiting human approval
- `ExecutionPlan` captures the original request context for deferred execution

**Audit** (`dbflux_audit`):
- `AuditService` delegates to `AuditRepository` in `dbflux_storage` (`~/.local/share/dbflux/dbflux.db`, `aud_audit_events` table)
- Events use `EventRecord` from `dbflux_core::observability` — structured fields for category, severity, outcome, actor type, connection, object, details, and error context
- Events are emitted through `EventSink` trait; service layers inject `Arc<dyn EventSink>` rather than calling `AuditService` directly
- Categories: `Query`, `Connection`, `Hook`, `Script`, `Mcp`, `Governance`, `Config`, `System`
- Before storage: validates required category-specific fields, fingerprints query text as SHA256 (query text never stored by default), redacts sensitive values, enforces 64 KiB detail payload limit
- `AuditQueryFilter` for querying by actor, tool, category, action, outcome, date range, free text, and correlation ID
- Export to JSON/CSV via `AuditExportFormat`; `export_extended()` includes all DTO fields including `details_json`
- Retention purge: `AuditService::purge_old_events(days, batch_size)` — batched to avoid long write transactions
- See `docs/AUDIT.md` for full event schema, required fields, and usage patterns

**MCP Runtime** (`dbflux_mcp/runtime.rs`):
- `McpRuntime` implements `McpGovernanceService` trait
- Integrates policy engine, approval service, and audit service
- Emits `McpRuntimeEvent` for UI updates (clients/roles/policies changed, pending executions, audit)
- Tool catalog (`tool_catalog.rs`) defines canonical MCP tools and deferred tools

**Standalone Server** (`dbflux_mcp_server`):
- Exposed as `dbflux mcp --client-id <id>` for AI clients
- JSON-RPC over stdin/stdout transport
- `ConnectionCache` plus serialized connection setup prevent request-scoped PostgreSQL teardown and duplicate-connect races
- Same governance stack as in-app MCP
- `preview_mutation` is strictly read-only; unsafe `preview_ddl` is intentionally not exposed until DBFlux has a safe non-mutating DDL preview path

**UI Integration**:
- `McpApprovalsView` (`crates/dbflux_ui/src/ui/document/governance.rs`) for reviewing pending executions
- `mcp_section.rs` in Settings for trusted clients, roles, policies, and audit log
- `LoginModal` and `SsoWizard` overlays for AWS SSO authentication flow

## Data Flow

- Startup: `main` creates `AppState` and `Workspace`, restores the previous session (tabs from `session.json`), and opens the main window. If no tabs are restored, focus defaults to the sidebar (crates/dbflux/src/main.rs, crates/dbflux_ui/src/ui/views/workspace/).
- External driver bootstrap: at startup, DBFlux reads `cfg_services` from `~/.local/share/dbflux/dbflux.db`, probes each service, and only registers services that complete the RPC handshake (`Hello`) successfully.
- Connect flow: `AppState::prepare_pipeline_input` builds a provider-agnostic pre-connect pipeline input. The pipeline runs auth/session validation, dynamic value resolution, and managed/direct access setup before driver connect + schema fetch. Supports form-based configuration, direct URI input, optional proxy/SSH, and managed access (`aws-ssm`). Connection hooks still run at each phase (PreConnect, PostConnect, PreDisconnect, PostDisconnect).
- Query flow: `CodeDocument` submits database queries to a `Connection` implementation when the active `QueryLanguage` supports connection context. The query language (SQL/MongoDB/etc) is determined by driver metadata. Results are rendered in result tabs within the document. Dangerous queries (DELETE without WHERE, DROP, TRUNCATE) trigger confirmation dialogs (handled in `code/execution.rs`).
- Script flow: `CodeDocument` executes Lua, Python, and Bash documents as script hooks rather than database queries. Script runs create a local output channel, stream live text into a document-owned buffer, and keep the final output as a text result when execution completes.
- View mode selection: `DataGridPanel` (in `document/data_grid_panel/`) automatically selects appropriate view mode based on database category—Table view for relational databases, Document tree view for document databases like MongoDB and DynamoDB, key-value view for Redis. Context menus include "Copy as Query" for generating driver-specific mutation statements/envelopes via `QueryGenerator`.
- Query preview: `SqlPreviewModal` (in `overlays/sql_preview_modal.rs`) routes relational read/DML previews through `QueryGenerator` for row, table, and view previews, while DDL stays on `CodeGenerator`. Non-SQL languages (MongoDB, Redis) still use generic preview mode with static text and language-specific syntax highlighting.
- Schema refresh: `Workspace::refresh_schema` runs `Connection::schema` on a background executor and updates `AppState` (crates/dbflux_ui/src/ui/views/workspace/).
- Lazy loading: Drivers fetch table/collection metadata (columns, indexes) on-demand when items are expanded in sidebar, not during initial connection (performance optimization for large databases).
- History flow: completed queries are stored in `HistoryStore`, persisted to JSON, and accessible via the history modal (crates/dbflux_core/src/storage/history.rs).
- Saved queries flow: users can save queries with names via `SavedQueryStore`; the history modal (Ctrl+P) allows browsing, searching, and loading saved queries (crates/dbflux_core/src/storage/saved_query.rs).

## Keyboard & Focus Architecture

- Keymap system: `crates/dbflux_ui/src/keymap/` defines `Command` enum (`command.rs`), context-aware key bindings (`defaults.rs`), and `FocusTarget` for panel routing (`focus.rs`). Domain command types live in `crates/dbflux_app/src/keymap/`.
- Command dispatch: `Workspace` implements `CommandDispatcher` trait; `dispatch()` in `views/workspace/dispatch.rs` routes commands based on `focus_target` (Document, Sidebar, BackgroundTasks).
- Document-focused design: FocusTarget was simplified from Editor/Results/Sidebar/BackgroundTasks to Document/Sidebar/BackgroundTasks, letting documents manage their own internal focus state.
- Focus layers: Each context has its own keymap layer with vim-style bindings (j/k/h/l navigation).
- Panel focus modes: Complex panels like data tables have internal focus state machines (`FocusMode::Table`/`Toolbar`, `EditState::Navigating`/`Editing`) to handle nested keyboard navigation.
- Mouse/keyboard sync: Mouse handlers update focus state to keep keyboard and mouse navigation consistent; a `switching_input` flag prevents race conditions during input blur events.

## External Integrations

- PostgreSQL: `tokio-postgres` client with optional TLS, cancellation support, lazy schema loading, and URI connection mode (crates/dbflux_driver_postgres/src/driver.rs).
- MySQL/MariaDB: `mysql` crate with dual connection architecture (sync for schema, async for queries), lazy schema loading, and URI connection mode (crates/dbflux_driver_mysql/src/driver.rs).
- SQLite: `rusqlite` file-based connections with lazy schema loading (crates/dbflux_driver_sqlite/src/driver.rs).
- MongoDB: `mongodb` async driver with BSON handling, query parser for `db.collection.method()` syntax, collection/index discovery, document CRUD, shell query generation, and collection description support for MCP/UI metadata workflows (crates/dbflux_driver_mongodb/src/driver.rs).
- Redis: `redis` driver with key-value API for all Redis types, variadic commands, keyspace support, key scanning, and command generation (crates/dbflux_driver_redis/src/driver.rs).
- DynamoDB: `aws-sdk-dynamodb` driver with AWS profile/region support for remote DynamoDB, plus optional endpoint override for local emulators and tests (crates/dbflux_driver_dynamodb/src/driver.rs).
- AWS auth stack: `dbflux_aws` provides AWS SSO/shared/static auth providers, SSO login orchestration, account/role discovery, and `~/.aws/config` profile write-back for newly saved auth profiles.
- Local IPC/RPC: `interprocess` sockets + versioned envelopes for app control and external driver communication (`crates/dbflux_ipc/`, `crates/dbflux_driver_ipc/`, `crates/dbflux_driver_host/`). Auth tokens are managed by `dbflux_ipc/src/auth.rs`.
- Proxy: SOCKS5/HTTP CONNECT tunnels via `dbflux_tunnel_core::Tunnel` (crates/dbflux_proxy/src/lib.rs).
- SSH: `ssh2` sessions with local TCP forwarding via `dbflux_tunnel_core::Tunnel` (crates/dbflux_ssh/src/lib.rs).
- OS keyring: optional secret storage for passwords, SSH passphrases, and proxy credentials (crates/dbflux_core/src/storage/secrets.rs).
- Export: shape-based multi-format export — CSV, JSON (pretty/compact), Text, Binary (raw/hex/base64) via `dbflux_export` (`lib.rs`, `binary.rs`, `csv.rs`, `json.rs`, `text.rs`).

## Configuration

- Workspace settings: `Cargo.toml` defines workspace members and shared dependencies.
- App features: `crates/dbflux/Cargo.toml` gates `sqlite`, `postgres`, `mysql`, `mongodb`, `redis`, `dynamodb`, `lua`, and `aws` (enabled by default in this branch).
- Runtime data: All runtime configuration is stored in `~/.local/share/dbflux/dbflux.db` (single SQLite file).
  - `cfg_connection_profiles` + child tables (auth, proxy, SSH bindings)
  - `cfg_auth_profiles` (provider-agnostic auth profile storage)
  - `cfg_ssh_tunnel_profiles`, `cfg_proxy_profiles`
  - `cfg_hooks`, `cfg_hook_bindings`
  - `cfg_services`, `cfg_service_args`, `cfg_service_env` (external RPC services)
  - `cfg_governance_*` tables (roles, policies, trusted clients)
  - `cfg_drivers` (per-driver settings overrides)
  - `cfg_folders` (connection tree organization)
  - `st_sessions`, `st_tabs`, `st_query_history`, `st_saved_queries`, `st_recent_items`, `st_ui_state`
  - `aud_audit_events`, `aud_audit_entities`, `aud_audit_attributes`
  - `sys_migrations`, `sys_legacy_imports`
- Legacy JSON import: On first startup, `dbflux_storage/src/legacy.rs` imports existing JSON files into SQLite if they exist:
  - `~/.config/dbflux/profiles.json` → `cfg_connection_profiles`
  - `~/.config/dbflux/auth_profiles.json` → `cfg_auth_profiles`
  - `~/.config/dbflux/ssh_tunnels.json` → `cfg_ssh_tunnel_profiles`
  - `~/.config/dbflux/config.json` (rpc_services only) → `cfg_services`
  - Import is idempotent (tracked in `sys_legacy_imports`)
- Session data (data dir):
  - `sessions/` scratch and shadow files for auto-save (crates/dbflux_core/src/storage/session.rs).
  - `scripts/` user scripts folder (crates/dbflux_core/src/config/scripts_directory.rs).
- Secrets: passwords stored in OS keyring; references derived from profile IDs. `HasSecretRef` trait unifies SSH tunnel and proxy secret operations (crates/dbflux_core/src/storage/secrets.rs, crates/dbflux_core/src/storage/secret_manager.rs).

## Build & Deploy

- Build: `cargo build -p dbflux --features sqlite,postgres,mysql,mongodb,redis,dynamodb,aws` or `--release` (AGENTS.md).
- Run: `cargo run -p dbflux --features sqlite,postgres,mysql,mongodb,redis,dynamodb,aws` (AGENTS.md).
- Test: `cargo test --workspace` (AGENTS.md).
- Lint/format: `cargo clippy --workspace -- -D warnings`, `cargo fmt --all` (AGENTS.md).
- Nix: `nix build` or `nix run` using flake.nix; `nix develop` for dev shell.
- Arch Linux: `makepkg -si` using scripts/PKGBUILD.
- Linux installer: `curl -fsSL .../install.sh | bash` downloads and installs release.
- Releases: GitHub Actions workflow builds Linux amd64/arm64, macOS amd64/arm64, and Windows amd64, with optional GPG signing, publishes to GitHub Releases.
- Deployment model: desktop GUI app; no server runtime in this repo.
