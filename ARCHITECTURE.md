# Architecture

## Overview

- DBFlux is a keyboard-first database client built with Rust and GPUI, focused on fast workflows and a clean desktop UI (README.md).
- The repo is a Rust workspace with a UI app crate plus shared core types, driver implementations, and supporting libraries (Cargo.toml, crates/).
- Supports multiple database paradigms: relational (SQL), document (MongoDB), key-value, graph, time-series, and wide-column stores.

## Tech Stack

- Language: Rust 2024 edition (crates/dbflux/Cargo.toml).
- UI: `gpui`, `gpui-component` (Cargo.toml).
- Databases: `tokio-postgres` (PostgreSQL), `rusqlite` (SQLite), `mysql` (MySQL/MariaDB), `mongodb` (MongoDB), `redis` (Redis) (Cargo.toml).
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
      sql_query.rs          # Query editor with language-aware syntax (SQL/MongoDB/etc)
      sql_query/file_ops.rs # Auto-save, scratch/shadow file management
      sql_query/context_bar.rs  # Execution context dropdowns (connection/database/schema)
      sql_query/render.rs   # Toolbar and editor rendering
      sql_query/execution.rs # Query execution flow
      sql_query/completion.rs # Language-aware autocompletion
      sql_query/diagnostics.rs # Live query diagnostics
      sql_query/focus.rs    # Internal focus management
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
    src/ui/history_modal.rs # Recent/saved queries modal
    src/ui/icons/           # SVG icon system (AppIcon enum)
    src/keymap/             # Keyboard system
      command.rs            # Command enum with all app actions
      defaults.rs           # Context-aware key bindings
      focus.rs              # FocusTarget enum for panel routing
  dbflux_core/              # Traits, core types, storage, errors
    src/traits.rs           # DbDriver + Connection traits
    src/driver_capabilities.rs  # DatabaseCategory, QueryLanguage, DriverCapabilities, DriverMetadata
    src/driver_form.rs      # Dynamic form definitions per driver
    src/error_formatter.rs  # ErrorFormatter trait for driver-specific error messages
    src/profile.rs          # Connection/SSH profiles
    src/connection_tree.rs  # Folder/connection tree model
    src/connection_tree_store.rs  # Tree persistence (JSON)
    src/store.rs            # Profile and tunnel stores (JSON)
    src/history.rs          # History persistence
    src/saved_query.rs      # Saved queries persistence
    src/task.rs             # Background task tracking
    src/schema.rs           # Schema types (tables, collections, indexes, FKs)
    src/schema_builder.rs   # Builder helpers for schema construction
    src/crud.rs             # CRUD mutation types for all database paradigms
    src/key_value.rs        # Key-value operation types (Hash, Set, List, ZSet, Stream)
    src/query_generator.rs  # QueryGenerator trait and MutationRequest routing
    src/language_service.rs # Dangerous query detection (SQL, MongoDB, Redis)
    src/session_store.rs    # Session persistence (scratch/shadow files, manifest)
    src/scripts_directory.rs # Scripts folder tree (file/folder CRUD)
    src/execution_context.rs # Per-tab execution context (connection/database/schema)
    src/session_facade.rs   # Session facade for connection management
    src/sql_dialect.rs      # SqlDialect trait for SQL flavor differences
    src/sql_generation.rs   # SQL INSERT/UPDATE/DELETE generation
    src/sql_query_builder.rs  # SqlQueryBuilder for safe query construction
    src/code_generation.rs  # Code generation utilities
    src/table_browser.rs    # Table browsing state and pagination
    src/value.rs            # Generic Value type for cross-database data
    src/data_view.rs        # DataViewMode (Table/Document) abstraction
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
  dbflux_ssh/               # SSH tunnel support
  dbflux_export/            # Export (CSV, JSON, Text, Binary)
```

## Core Components

### Application Layer

- App entry point: `crates/dbflux/src/main.rs` initializes logging, theme, and main GPUI window.
- Global app state: `crates/dbflux/src/app.rs` owns drivers, profiles, active connections, history, task manager, and secret store access.
- Workspace UI shell: `crates/dbflux/src/ui/workspace.rs` wires panes (sidebar/dock, document area, bottom dock), command palette, and focus routing.

### Document System

`crates/dbflux/src/ui/document/` implements a tab-based document architecture:

- `DocumentHandle` manages document lifecycle as GPUI entities
- `SqlQueryDocument` provides language-aware query editing (SQL/MongoDB/etc) with multiple result tabs (Ctrl+Enter to run, Ctrl+Shift+Enter for new tab). Supports file-backed script documents with execution context dropdowns (connection/database/schema).
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

- Sidebar: `crates/dbflux/src/ui/sidebar.rs` displays two tabs — Connections (schema tree with folder organization, drag-drop, multi-selection) and Scripts (file/folder management for saved query files). Switch tabs with `q` or `e` keys. Shows tables/collections, columns, indexes per database category with lazy loading.
- Sidebar dock: `crates/dbflux/src/ui/dock/sidebar_dock.rs` provides collapsible, resizable sidebar with ToggleSidebar command (Ctrl+B).
- Connection tree: `crates/dbflux_core/src/connection_tree.rs` models folders and connections as a tree structure with persistence via `connection_tree_store.rs`.

### Driver System

- **Driver capabilities**: `crates/dbflux_core/src/driver_capabilities.rs` defines:
  - `DatabaseCategory`: Relational, Document, KeyValue, Graph, TimeSeries, WideColumn
  - `QueryLanguage`: SQL, MongoQuery, RedisCommands, Cypher, InfluxQuery, CQL (with editor mode, placeholder, comment prefix)
  - `DriverCapabilities`: bitflags for features like PAGINATION, TRANSACTIONS, NESTED_DOCUMENTS, etc.
  - `DriverMetadata`: static driver info (id, name, category, query_language, capabilities, icon)
- **Error formatting**: `crates/dbflux_core/src/error_formatter.rs` provides `ErrorFormatter` trait for driver-specific error messages with context (detail, hint, column, table, constraint).
- Core domain API: `crates/dbflux_core/src/traits.rs` defines `DbDriver`, `Connection`, SQL generation, and cancellation contracts.
- **Query generation**: `crates/dbflux_core/src/query_generator.rs` defines `QueryGenerator` trait with `supported_categories()` and `generate_mutation(&MutationRequest)`. Each driver crate implements its own generator (SQL via `SqlMutationGenerator`, MongoDB via `MongoShellGenerator`, Redis via `RedisCommandGenerator`). The UI accesses generators through `Connection::query_generator()`.
- Driver forms: `crates/dbflux_core/src/driver_form.rs` defines dynamic form schemas that drivers provide for connection configuration. Supports both form-based and URI connection modes.
- **Driver/UI decoupling**: The UI never checks driver IDs directly. Instead, it uses `DriverMetadata` abstractions (`DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`) to adapt behavior. This allows new drivers to work automatically without UI changes.

### SQL Generation

- **SQL dialect**: `crates/dbflux_core/src/sql_dialect.rs` defines `SqlDialect` trait for database-specific SQL syntax (quoting, LIMIT/OFFSET, type mapping).
- **SQL generation**: `crates/dbflux_core/src/sql_generation.rs` provides INSERT/UPDATE/DELETE statement generation.
- **Query builder**: `crates/dbflux_core/src/sql_query_builder.rs` offers `SqlQueryBuilder` for safe, parameterized query construction.

### CRUD Operations

- **Mutation types**: `crates/dbflux_core/src/crud.rs` defines `MutationRequest` enum covering all database paradigms:
  - SQL: INSERT/UPDATE/DELETE with WHERE clauses
  - Document: insertOne/updateOne/deleteOne/deleteMany
  - Key-Value: SET/DELETE/HASH_SET/SET_ADD/LIST_PUSH/ZSET_ADD and their remove counterparts, plus STREAM_ADD
- **Key-value types**: `crates/dbflux_core/src/key_value.rs` defines Vec-based request structs for variadic Redis commands (e.g., `HashSetRequest.fields: Vec<(String, String)>`, `SetAddRequest.members: Vec<String>`).
- **Query safety**: `crates/dbflux_core/src/language_service.rs` detects dangerous queries across all languages (SQL DELETE/DROP/TRUNCATE, MongoDB deleteMany/drop, Redis FLUSHALL/FLUSHDB/KEYS) and prompts for confirmation before execution.

### Storage & Configuration

- Profiles + secrets: `crates/dbflux_core/src/profile.rs` and `crates/dbflux_core/src/secrets.rs` define connection/SSH profiles and keyring integration.
- Storage: `crates/dbflux_core/src/store.rs`, `crates/dbflux_core/src/history.rs`, and `crates/dbflux_core/src/saved_query.rs` persist JSON data in the config dir.
- Session persistence: `crates/dbflux_core/src/session_store.rs` manages scratch/shadow files and a session manifest in `~/.local/share/dbflux/sessions/` for tab restore on startup.
- Scripts directory: `crates/dbflux_core/src/scripts_directory.rs` manages a user scripts folder with file/folder CRUD, import, and move operations.
- Execution context: `crates/dbflux_core/src/execution_context.rs` tracks per-tab connection, database, and schema selection; serialized as annotation comments in saved files.
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
- SSH tunneling: `crates/dbflux_ssh/src/lib.rs` establishes SSH sessions and runs a local port forwarder.
- Export: `crates/dbflux_export/src/lib.rs` provides shape-based export (CSV, JSON pretty/compact, Text, Binary/Hex/Base64). Format availability is determined by `QueryResultShape`, not by driver.
- Icon system: `crates/dbflux/src/ui/icons/mod.rs` centralized AppIcon enum with embedded SVG assets.

## Data Flow

- Startup: `main` creates `AppState` and `Workspace`, restores the previous session (tabs from `session.json`), and opens the main window. If no tabs are restored, focus defaults to the sidebar (crates/dbflux/src/main.rs, crates/dbflux/src/ui/workspace.rs).
- Connect flow: `AppState::prepare_connect_profile` selects a driver and builds `ConnectProfileParams`, which connects and fetches schema (crates/dbflux/src/app.rs). Supports both form-based configuration and direct URI input.
- Query flow: SqlQueryDocument submits queries to a `Connection` implementation; the query language (SQL/MongoDB/etc) is determined by driver metadata. Results are rendered in result tabs within the document. Dangerous queries (DELETE without WHERE, DROP, TRUNCATE) trigger confirmation dialogs.
- View mode selection: `DataGridPanel` automatically selects appropriate view mode based on database category—Table view for relational databases, Document tree view for document databases like MongoDB. Context menus include "Copy as Query" for generating INSERT/UPDATE/DELETE via the connection's `QueryGenerator`.
- Query preview: `SqlPreviewModal` operates in dual mode—SQL mode with regeneration and options panel, or generic mode for non-SQL languages (MongoDB, Redis) with static text and language-specific syntax highlighting.
- Schema refresh: `Workspace::refresh_schema` runs `Connection::schema` on a background executor and updates `AppState` (crates/dbflux/src/ui/workspace.rs).
- Lazy loading: Drivers fetch table/collection metadata (columns, indexes) on-demand when items are expanded in sidebar, not during initial connection (performance optimization for large databases).
- History flow: completed queries are stored in `HistoryStore`, persisted to JSON, and accessible via the history modal (crates/dbflux_core/src/history.rs).
- Saved queries flow: users can save queries with names via `SavedQueryStore`; the history modal (Ctrl+P) allows browsing, searching, and loading saved queries (crates/dbflux_core/src/saved_query.rs).

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
- SSH: `ssh2` sessions with local TCP forwarding (crates/dbflux_ssh/src/lib.rs).
- OS keyring: optional secret storage for passwords and SSH passphrases (crates/dbflux_core/src/secrets.rs).
- Export: shape-based multi-format export — CSV, JSON (pretty/compact), Text, Binary (raw/hex/base64) via `dbflux_export` (crates/dbflux_export/src/lib.rs).

## Configuration

- Workspace settings: `Cargo.toml` defines workspace members and shared dependencies.
- App features: `crates/dbflux/Cargo.toml` gates `sqlite`, `postgres`, `mysql`, `mongodb`, and `redis` drivers (all enabled by default).
- Runtime data (config dir via `dirs::config_dir`):
  - `profiles.json` and `ssh_tunnels.json` (crates/dbflux_core/src/store.rs).
  - `history.json` for query history (crates/dbflux_core/src/history.rs).
  - `saved_queries.json` for user-saved queries (crates/dbflux_core/src/saved_query.rs).
- Session data (data dir via `dirs::data_dir`):
  - `sessions/session.json` manifest of open tabs (crates/dbflux_core/src/session_store.rs).
  - `sessions/*.sql` scratch and shadow files for auto-save (crates/dbflux_core/src/session_store.rs).
  - `scripts/` user scripts folder (crates/dbflux_core/src/scripts_directory.rs).
- Secrets: passwords stored in OS keyring; references derived from profile IDs (crates/dbflux_core/src/secrets.rs).

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
