# Architecture

## Overview

- DBFlux is a keyboard-first database client built with Rust and GPUI, focused on fast workflows and a clean desktop UI (README.md).
- The repo is a Rust workspace with a UI app crate plus shared core types, driver implementations, and supporting libraries (Cargo.toml, crates/).
- Supports multiple database paradigms: relational (SQL), document (MongoDB), key-value, graph, time-series, and wide-column stores.

## Tech Stack

- Language: Rust 2024 edition (crates/dbflux/Cargo.toml).
- UI: `gpui`, `gpui-component` (Cargo.toml).
- Databases: `tokio-postgres` (PostgreSQL), `rusqlite` (SQLite), `mysql` (MySQL/MariaDB), `mongodb` (MongoDB), `redis` (Redis) (Cargo.toml).
- IPC/RPC: `interprocess` local sockets + `bincode` message framing (`dbflux_ipc`, `dbflux_driver_ipc`, `dbflux_driver_host`).
- SSH: `ssh2` via `dbflux_ssh` (crates/dbflux_ssh/src/lib.rs).
- Export: `csv` + `hex` + `base64` + `serde_json` via `dbflux_export` (crates/dbflux_export/src/lib.rs).
- Serialization/config: `serde`, `serde_json`, `dirs` (Cargo.toml).
- Logging: `log`, `env_logger` (crates/dbflux/src/main.rs).

## Directory Structure

```
crates/
  dbflux/                   # GPUI app + UI composition
    src/main.rs             # Application entry point
    src/app.rs              # Global state, drivers, profiles, history
    src/ui/                 # UI panels, windows, theme
    src/ui/workspace.rs     # Main layout, command dispatch, focus routing
    src/ui/sidebar.rs       # Connection tree with folders, drag-drop, multi-select
    src/ui/dock/            # Resizable dock panels
      sidebar_dock.rs       # Collapsible, resizable sidebar
      mod.rs                # Bottom dock support
    src/ui/document/        # Document-based tab system (like VS Code/DBeaver)
      mod.rs                # Document exports and shared types
      handle.rs             # DocumentHandle for entity management
      types.rs              # DocumentId, DocumentKind, DocumentState
      code/mod.rs           # CodeDocument: query/script editor with language-aware behavior
      code/file_ops.rs      # Auto-save, scratch/shadow file management
      code/context_bar.rs   # Execution context dropdowns (connection/database/schema)
      code/render.rs        # Toolbar, editor, and live output rendering
      code/execution.rs     # Query and script execution flow
      code/completion.rs    # Language-aware autocompletion
      code/diagnostics.rs   # Live query diagnostics
      code/focus.rs         # Internal focus management
      code/live_output.rs   # Document-owned streamed script output buffer
      data_document.rs      # Standalone data browsing document
      tab_manager.rs        # MRU tab ordering and activation
      tab_bar.rs            # Visual tab bar rendering
      data_grid_panel.rs    # Data grid with table/document view modes
      data_view.rs          # DataViewMode abstraction (Table vs Document)
    src/ui/editor.rs        # Code editor component
    src/ui/sql_preview_modal.rs  # SQL/query preview modal (dual-mode: SQL and generic)
    src/ui/dangerous_query.rs  # Query safety analysis and confirmation
    src/ui/toast.rs         # Custom toast notification system
    src/ui/cell_editor_modal.rs  # Modal editor for JSON/long text cells
    src/ui/components/data_table/  # Custom virtualized data table
      table.rs              # Main table component with phantom scroller
      state.rs              # Table state management
      model.rs              # CellValue and data model
      selection.rs          # Selection handling
      events.rs             # Event handling
      clipboard.rs          # Copy/paste support
      theme.rs              # Table styling
    src/ui/components/document_tree/  # Hierarchical document/JSON viewer
      state.rs              # Tree state with cursor, expansion, search
      tree.rs               # Tree rendering with keyboard navigation
      node.rs               # Node types (document, field, array item)
      events.rs             # Document tree events (selection, context menu)
    src/ui/components/tree_nav.rs  # Reusable tree navigation component
    src/ui/components/form_renderer.rs # Generic form field rendering
    src/ui/windows/settings/      # Settings window sections
      general.rs            # General settings (theme, safety toggles)
      proxies.rs            # Proxy CRUD form with FormGridNav
      ssh_tunnels.rs        # SSH tunnel CRUD form with FormGridNav
      hooks.rs              # Hook definitions CRUD
      drivers.rs            # Per-driver settings overrides
      rpc_services.rs       # External RPC service management
      form_nav.rs           # FormGridNav<F> generic 2D grid navigation
    src/ui/windows/connection_manager/
      hooks_tab.rs          # Per-profile hook bindings
      proxy.rs              # Proxy tab (selector, details, clear)
    src/proxy.rs            # create_proxy_tunnel callback for CreateTunnelFn
    src/ui/history_modal.rs # Recent/saved queries modal
    src/ui/icons/           # SVG icon system (AppIcon enum)
    src/keymap/             # Keyboard system
      command.rs            # Command enum with all app actions
      defaults.rs           # Context-aware key bindings
      focus.rs              # FocusTarget enum for panel routing
  dbflux_core/              # Traits, core types, storage, errors
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
      generator.rs          # QueryGenerator trait and MutationRequest routing
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
      app.rs                # Runtime config for external RPC services (`config.json`)
      refresh_policy.rs     # Schema refresh policy
      scripts_directory.rs  # Scripts folder tree (file/folder CRUD)
    src/facade/             # Session facade
      session.rs            # Session facade for connection management
  dbflux_ipc/               # Versioned IPC contracts and framing
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
    src/command_generator.rs  # Redis command generator (SET, HSET, SADD, etc.)
  dbflux_lua/               # Embedded Lua runtime for in-process hooks
    src/executor.rs         # Lua HookExecutor implementation
    src/engine.rs           # Lua VM creation and shared runtime state
    src/api/dbflux.rs       # dbflux.log/env/process Lua APIs
  dbflux_tunnel_core/       # Shared RAII tunnel infrastructure
    src/lib.rs              # Tunnel, TunnelConnector, ForwardingConnection<R>
  dbflux_proxy/             # SOCKS5/HTTP CONNECT proxy tunnel
    src/lib.rs              # ProxyTunnelConfig, SOCKS5/HTTP handshake, tunnel loop
  dbflux_ssh/               # SSH tunnel support
  dbflux_export/            # Export (CSV, JSON, Text, Binary)
  dbflux_test_support/      # Docker containers and fixtures for integration tests
    src/containers.rs       # Docker container lifecycle (Postgres, MySQL, MongoDB, Redis)
    src/fixtures.rs         # Test fixture helpers
    src/fake_driver.rs      # FakeDriver for unit tests
```

## Core Components

### Application Layer

- App entry point: `crates/dbflux/src/main.rs` initializes logging, theme, and main GPUI window.
- Global app state: `crates/dbflux/src/app.rs` owns drivers, profiles, active connections, history, task manager, and secret store access.
- Workspace UI shell: `crates/dbflux/src/ui/workspace.rs` wires panes (sidebar/dock, document area, bottom dock), command palette, and focus routing.

### Document System

`crates/dbflux/src/ui/document/` implements a tab-based document architecture:

- `DocumentHandle` manages document lifecycle as GPUI entities
- `CodeDocument` provides language-aware editing for queries and scripts (SQL/MongoDB/Redis/Lua/Python/Bash) with multiple result tabs and live output for scripts. Connection/database/schema controls are only shown for languages that support connection context.
- Auto-save: tabs auto-save to scratch files (untitled) or shadow files (file-backed) on a 2-second debounce. Explicit Ctrl+S writes to the original file. Tabs close without warnings.
- Session restore: `SessionStore` persists a manifest of open tabs to `~/.local/share/dbflux/sessions/`. On startup, all tabs are restored with conflict detection for externally modified files.
- `DataDocument` enables standalone data browsing independent of queries
- `TabManager` tracks MRU (Most Recently Used) order for tab switching
- `DataGridPanel` renders data with switchable view modes (Table for SQL, Document tree for MongoDB)
- Duplicate prevention: opening an already-open table/collection focuses the existing tab

### Data Visualization

- **Data table**: `crates/dbflux/src/ui/components/data_table/` custom virtualized table with sorting, selection, horizontal scrolling via phantom scroller pattern, keyboard navigation, column resizing, and context menu with CRUD operations.
- **Document tree**: `crates/dbflux/src/ui/components/document_tree/` hierarchical JSON/BSON viewer for document databases with keyboard navigation (j/k/h/l), search (Ctrl+F or /), collapsible nodes, and view modes (Keys Only, Keys+Preview, Full Values).
- Cell editor modal: `crates/dbflux/src/ui/cell_editor_modal.rs` provides a modal editor for JSON columns and long/multiline text, with JSON validation and formatting.

### Schema & Navigation

- Sidebar: `crates/dbflux/src/ui/sidebar.rs` displays two tabs — Connections (schema tree with folder organization, drag-drop, multi-selection) and Scripts (file/folder management for saved query files, script hooks, and other user files). Switch tabs with `q` or `e` keys. Shows tables/collections, columns, indexes per database category with lazy loading.
- Sidebar dock: `crates/dbflux/src/ui/dock/sidebar_dock.rs` provides collapsible, resizable sidebar with ToggleSidebar command (Ctrl+B).
- Connection tree: `crates/dbflux_core/src/connection/tree.rs` models folders and connections as a tree structure with persistence via `connection_tree_store.rs`.

### Driver System

- **Driver capabilities**: `crates/dbflux_core/src/driver/capabilities.rs` defines:
  - `DatabaseCategory`: Relational, Document, KeyValue, Graph, TimeSeries, WideColumn
  - `QueryLanguage`: SQL, MongoQuery, RedisCommands, Cypher, InfluxQuery, CQL (with editor mode, placeholder, comment prefix)
  - `DriverCapabilities`: bitflags for features like PAGINATION, TRANSACTIONS, NESTED_DOCUMENTS, etc.
  - `DriverMetadata`: static driver info (id, name, category, query_language, capabilities, icon)
- **Error formatting**: `crates/dbflux_core/src/core/error_formatter.rs` provides `ErrorFormatter` trait for driver-specific error messages with context (detail, hint, column, table, constraint).
- Core domain API: `crates/dbflux_core/src/core/traits.rs` defines `DbDriver`, `Connection`, SQL generation, and cancellation contracts.
- **Query generation**: `crates/dbflux_core/src/query/generator.rs` defines `QueryGenerator` trait with `supported_categories()` and `generate_mutation(&MutationRequest)`. Each driver crate implements its own generator (SQL via `SqlMutationGenerator`, MongoDB via `MongoShellGenerator`, Redis via `RedisCommandGenerator`). The UI accesses generators through `Connection::query_generator()`.
- Driver forms: `crates/dbflux_core/src/driver/form.rs` defines dynamic form schemas that drivers provide for connection configuration. Supports both form-based and URI connection modes.
- **Driver/UI decoupling**: The UI never checks driver IDs directly. Instead, it uses `DriverMetadata` abstractions (`DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`) to adapt behavior. This allows new drivers to work automatically without UI changes.

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

- Settings is organized into 7 sections: General, Keybindings, Proxies, SSH Tunnels, Services, Hooks, Drivers.
- Sidebar uses `TreeNav` component with collapsible Network/Connection categories.
- `UiStateStore` persists sidebar collapse state to `~/.local/share/dbflux/state.json`.
- Proxy and SSH tunnel forms use `FormGridNav<F>` for keyboard-driven 2D grid navigation.
- Drivers section shows per-driver settings overrides filtered by `DatabaseCategory`.

### IPC/RPC Integration

- `crates/dbflux_ipc/` defines versioned app-control and driver RPC contracts, transport framing, and cross-platform socket naming.
- `crates/dbflux/src/main.rs` uses app-control IPC for single-instance behavior (`Focus`, `OpenScript`) with protocol-version compatibility checks.
- `crates/dbflux_core/src/config/app.rs` loads `~/.config/dbflux/config.json` and exposes `rpc_services` runtime configuration.
- `crates/dbflux/src/app.rs` probes each configured RPC service at startup (`Hello`) and registers it as an in-memory driver key `rpc:<socket_id>`.
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

- Profiles + secrets: `crates/dbflux_core/src/connection/profile.rs` and `crates/dbflux_core/src/storage/secrets.rs` define connection/SSH/proxy profiles and keyring integration.
- Generic stores: `crates/dbflux_core/src/storage/json_store.rs` provides `JsonStore<T>` with type aliases (`ProfileStore`, `SshTunnelStore`, `ProxyStore`). `ItemManager<T>` in `connection/item_manager.rs` adds CRUD + auto-save; `ProxyManager` and `SshTunnelManager` are type aliases.
- Secret management: `SecretManager` uses `HasSecretRef` trait for generic keyring operations across SSH tunnels and proxy profiles.
- Storage: `crates/dbflux_core/src/storage/history.rs` and `crates/dbflux_core/src/storage/saved_query.rs` persist JSON data in the config dir.
- Session persistence: `crates/dbflux_core/src/storage/session.rs` manages scratch/shadow files and a session manifest in `~/.local/share/dbflux/sessions/` for tab restore on startup.
- UI state: `crates/dbflux_core/src/storage/ui_state.rs` persists sidebar collapse state to `~/.local/share/dbflux/state.json`.
- Scripts directory: `crates/dbflux_core/src/config/scripts_directory.rs` manages a user scripts folder with file/folder CRUD, import, and move operations.
- Execution context: `crates/dbflux_core/src/connection/context.rs` tracks per-tab connection, database, and schema selection; serialized as annotation comments in saved files.
- History modal: `crates/dbflux/src/ui/history_modal.rs` provides a unified modal for browsing recent queries and saved queries with search, favorites, and rename support.

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

### Supporting Components

- Toast system: `crates/dbflux/src/ui/toast.rs` custom implementation with auto-dismiss (4s) for success/info/warning toasts.
- Tunnel infrastructure: `crates/dbflux_tunnel_core/` provides RAII `Tunnel` with `TunnelConnector` trait and `ForwardingConnection<R>` bidirectional forwarder.
- Proxy tunneling: `crates/dbflux_proxy/` implements SOCKS5 and HTTP CONNECT proxy tunnels via `TunnelConnector`.
- SSH tunneling: `crates/dbflux_ssh/src/lib.rs` implements SSH tunnel via `TunnelConnector`, all operations serialized to one thread for libssh2 safety.
- Export: `crates/dbflux_export/src/lib.rs` provides shape-based export (CSV, JSON pretty/compact, Text, Binary/Hex/Base64). Format availability is determined by `QueryResultShape`, not by driver.
- Test support: `crates/dbflux_test_support/` provides Docker container management and fixtures for live integration tests across all drivers.
- Icon system: `crates/dbflux/src/ui/icons/mod.rs` centralized AppIcon enum with embedded SVG assets.

## Data Flow

- Startup: `main` creates `AppState` and `Workspace`, restores the previous session (tabs from `session.json`), and opens the main window. If no tabs are restored, focus defaults to the sidebar (crates/dbflux/src/main.rs, crates/dbflux/src/ui/workspace.rs).
- External driver bootstrap: at startup, DBFlux reads `~/.config/dbflux/config.json`, probes each `rpc_service`, and only registers services that complete the RPC handshake (`Hello`) successfully.
- Connect flow: `AppState::prepare_connect_profile` selects a driver and builds `ConnectProfileParams`, which optionally creates a proxy tunnel, then connects and fetches schema (crates/dbflux/src/app.rs). Supports form-based configuration, direct URI input, optional proxy tunneling, and SSH tunneling (mutually exclusive). Connection hooks run at each phase (PreConnect, PostConnect, PreDisconnect, PostDisconnect).
- Query flow: `CodeDocument` submits database queries to a `Connection` implementation when the active `QueryLanguage` supports connection context. The query language (SQL/MongoDB/etc) is determined by driver metadata. Results are rendered in result tabs within the document. Dangerous queries (DELETE without WHERE, DROP, TRUNCATE) trigger confirmation dialogs.
- Script flow: `CodeDocument` executes Lua, Python, and Bash documents as script hooks rather than database queries. Script runs create a local output channel, stream live text into a document-owned buffer, and keep the final output as a text result when execution completes.
- View mode selection: `DataGridPanel` automatically selects appropriate view mode based on database category—Table view for relational databases, Document tree view for document databases like MongoDB. Context menus include "Copy as Query" for generating INSERT/UPDATE/DELETE via the connection's `QueryGenerator`.
- Query preview: `SqlPreviewModal` operates in dual mode—SQL mode with regeneration and options panel, or generic mode for non-SQL languages (MongoDB, Redis) with static text and language-specific syntax highlighting.
- Schema refresh: `Workspace::refresh_schema` runs `Connection::schema` on a background executor and updates `AppState` (crates/dbflux/src/ui/workspace.rs).
- Lazy loading: Drivers fetch table/collection metadata (columns, indexes) on-demand when items are expanded in sidebar, not during initial connection (performance optimization for large databases).
- History flow: completed queries are stored in `HistoryStore`, persisted to JSON, and accessible via the history modal (crates/dbflux_core/src/storage/history.rs).
- Saved queries flow: users can save queries with names via `SavedQueryStore`; the history modal (Ctrl+P) allows browsing, searching, and loading saved queries (crates/dbflux_core/src/storage/saved_query.rs).

## Keyboard & Focus Architecture

- Keymap system: `crates/dbflux/src/keymap/` defines `Command` enum, `KeyChord` parsing, context-aware `KeymapLayer` bindings, and `FocusTarget` for panel routing.
- Command dispatch: `Workspace` implements `CommandDispatcher` trait; `dispatch()` routes commands based on `focus_target` (Document, Sidebar, BackgroundTasks).
- Document-focused design: FocusTarget was simplified from Editor/Results/Sidebar/BackgroundTasks to Document/Sidebar/BackgroundTasks, letting documents manage their own internal focus state.
- Focus layers: Each context has its own keymap layer with vim-style bindings (j/k/h/l navigation).
- Panel focus modes: Complex panels like data tables have internal focus state machines (`FocusMode::Table`/`Toolbar`, `EditState::Navigating`/`Editing`) to handle nested keyboard navigation.
- Mouse/keyboard sync: Mouse handlers update focus state to keep keyboard and mouse navigation consistent; a `switching_input` flag prevents race conditions during input blur events.

## External Integrations

- PostgreSQL: `tokio-postgres` client with optional TLS, cancellation support, lazy schema loading, and URI connection mode (crates/dbflux_driver_postgres/src/driver.rs).
- MySQL/MariaDB: `mysql` crate with dual connection architecture (sync for schema, async for queries), lazy schema loading, and URI connection mode (crates/dbflux_driver_mysql/src/driver.rs).
- SQLite: `rusqlite` file-based connections with lazy schema loading (crates/dbflux_driver_sqlite/src/driver.rs).
- MongoDB: `mongodb` async driver with BSON handling, query parser for `db.collection.method()` syntax, collection/index discovery, document CRUD, and shell query generation (crates/dbflux_driver_mongodb/src/driver.rs).
- Redis: `redis` driver with key-value API for all Redis types, variadic commands, keyspace support, key scanning, and command generation (crates/dbflux_driver_redis/src/driver.rs).
- Local IPC/RPC: `interprocess` sockets + versioned envelopes for app control and external driver communication (`crates/dbflux_ipc/`, `crates/dbflux_driver_ipc/`, `crates/dbflux_driver_host/`).
- Proxy: SOCKS5/HTTP CONNECT tunnels via `dbflux_tunnel_core::Tunnel` (crates/dbflux_proxy/src/lib.rs).
- SSH: `ssh2` sessions with local TCP forwarding via `dbflux_tunnel_core::Tunnel` (crates/dbflux_ssh/src/lib.rs).
- OS keyring: optional secret storage for passwords, SSH passphrases, and proxy credentials (crates/dbflux_core/src/storage/secrets.rs).
- Export: shape-based multi-format export — CSV, JSON (pretty/compact), Text, Binary (raw/hex/base64) via `dbflux_export` (crates/dbflux_export/src/lib.rs).

## Configuration

- Workspace settings: `Cargo.toml` defines workspace members and shared dependencies.
- App features: `crates/dbflux/Cargo.toml` gates `sqlite`, `postgres`, `mysql`, `mongodb`, and `redis` drivers (all enabled by default).
- Runtime data (config dir via `dirs::config_dir`):
  - `config.json` for external RPC services (`rpc_services` with socket id, command, args, env, startup timeout) (crates/dbflux_core/src/config/app.rs).
  - `profiles.json`, `ssh_tunnels.json`, and `proxies.json` (crates/dbflux_core/src/storage/json_store.rs).
  - `history.json` for query history (crates/dbflux_core/src/storage/history.rs).
  - `saved_queries.json` for user-saved queries (crates/dbflux_core/src/storage/saved_query.rs).
- Session data (data dir via `dirs::data_dir`):
  - `sessions/session.json` manifest of open tabs (crates/dbflux_core/src/storage/session.rs).
  - `sessions/` scratch and shadow files for auto-save (crates/dbflux_core/src/storage/session.rs).
  - `scripts/` user scripts folder (crates/dbflux_core/src/config/scripts_directory.rs).
  - `state.json` persisted UI state — sidebar collapse, etc. (crates/dbflux_core/src/storage/ui_state.rs).
- Secrets: passwords stored in OS keyring; references derived from profile IDs. `HasSecretRef` trait unifies SSH tunnel and proxy secret operations (crates/dbflux_core/src/storage/secrets.rs, crates/dbflux_core/src/storage/secret_manager.rs).

## Build & Deploy

- Build: `cargo build -p dbflux --features sqlite,postgres,mysql,mongodb,redis` or `--release` (AGENTS.md).
- Run: `cargo run -p dbflux --features sqlite,postgres,mysql,mongodb,redis` (AGENTS.md).
- Test: `cargo test --workspace` (AGENTS.md).
- Lint/format: `cargo clippy --workspace -- -D warnings`, `cargo fmt --all` (AGENTS.md).
- Nix: `nix build` or `nix run` using flake.nix; `nix develop` for dev shell.
- Arch Linux: `makepkg -si` using scripts/PKGBUILD.
- Linux installer: `curl -fsSL .../install.sh | bash` downloads and installs release.
- Releases: GitHub Actions workflow builds Linux amd64/arm64, macOS amd64/arm64, and Windows amd64, with optional GPG signing, publishes to GitHub Releases.
- Deployment model: desktop GUI app; no server runtime in this repo.
