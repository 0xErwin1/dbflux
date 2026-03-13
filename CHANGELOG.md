# Changelog

All notable changes to DBFlux will be documented in this file.

## [0.4.0-dev.8] – 2026-03-12

### Added

* Built-in DynamoDB driver with end-to-end app integration, including schema browse and mutation/query support
* In-app AWS SSO login flow and auth profile wizard, integrated across workspace overlays, Connection Manager, and Settings
* Provider-agnostic dynamic value source flow for managed access fields (SSM, Secrets Manager, and Parameter Store references)
* Language-specific script icons in the sidebar for Lua, Python, Bash, JavaScript, and InfluxDB files

### Changed

* Auth and managed access orchestration is now provider-driven via a runtime auth provider registry and generic pipeline stages
* Saving SSO/shared AWS auth profiles now writes compatible `[profile ...]` entries to `~/.aws/config` with idempotent updates
* Nix development builds now statically link OpenSSL for better portability outside Nix environments
* Driver crates now include dedicated README files documenting current features and limitations

### Fixed

* DynamoDB filter expression generation no longer emits redundant logical wrapping that could trigger `ValidationException` errors

---

## [0.4.0-dev.7] – 2026-03-07

### Added

* Native file browser button on the SQLite connection form so users can pick a database file instead of typing the full path
* Per-process authentication tokens for app-control IPC and driver RPC host handshakes, preventing unauthorized local process access

### Changed

* Credentials now use `SecretString` end-to-end across core, drivers, IPC handoff, and UI; plain strings are only exposed at explicit boundaries (keyring write, driver connect)
* `SecretStore::set()` accepts `&SecretString` instead of `&str`, pushing `expose_secret()` to the keyring boundary
* Detached async tasks that silently discarded `cx.update()` failures now log warnings through an `AsyncUpdateResultExt` trait (38 call sites across 9 files)

### Fixed

* SSH tunnels now enforce host key TOFU verification instead of accepting any host key
* Proxy tunnels enforce TLS when the target uses HTTPS, and `unreachable!` branches in proxy transport are replaced with safe fallbacks
* PostgreSQL URI `sslmode` parameter is now honored during connection
* URI passwords are sanitized before profile persistence to prevent accidental credential storage in plaintext config
* Lua VM memory is capped at 16 MiB to prevent runaway scripts from exhausting host memory
* Script rename validation rejects path traversal sequences
* IPC mutex operations replaced with explicit error handling instead of panic-prone `unwrap()`
* Proxy credentials are redacted in debug output
* Lua hook settings now warn when a hook has `process.run` capability enabled

---

## [0.4.0-dev.6] – 2026-03-07

### Added

* Script hooks now support Bash, Python, and embedded Lua with inline/file sources, interpreter overrides, and capability controls in Settings
* Code documents can run Bash, Python, and Lua files directly, with live output streaming while scripts execute
* New `dbflux_lua` crate provides sandboxed in-process Lua execution for hooks and editor-run scripts
* Dedicated style CI workflow now runs `cargo fmt --check` and `cargo clippy --workspace -- -D warnings`

### Changed

* The old SQL query document evolved into a language-aware code document with script-specific execution UI and shared live-output plumbing for hooks and manual runs
* Release workflow now waits for both test and style jobs before building artifacts
* New connections and SSH tunnels now save credentials to the system keyring by default unless explicitly disabled

### Fixed

* Detached pre-connect hooks now wait for an explicit ready signal and a reachable TCP endpoint before opening the database connection
* Hook-owned tunnel processes are cleaned up when startup fails or is cancelled, and MySQL `localhost` tunnel connections now consistently use IPv4
* Query context selectors now refresh when connection state changes, and per-database query cancellation targets the correct connection so databases can be reopened cleanly
* Typing `z` in the editor works again instead of toggling panels while text input is focused
* PostgreSQL retry setup and live integration tests now compile cleanly under current type inference requirements
* Restored the full `LICENSE-MIT` text in release artifacts

---

## [0.4.0-dev.5] – 2026-03-05

### Added

* Tab context menu: right-click or Ctrl+M on workspace tabs to Close, Close Others, Close All, Close to the Left, Close to the Right; supports j/k and arrow key navigation
* Sidebar: new folders auto-select, scroll into view, and enter rename mode immediately after creation
* Sidebar: "Duplicate" option in connection profile context menu clones the profile including keyring passwords into the same folder
* Shared `context_menu` component used by both sidebar and tab bar menus, replacing the old sidebar-specific implementation

### Changed

* `dbflux_core` reorganized from 50 flat files into 10 thematic subdirectories: `core/`, `driver/`, `schema/`, `sql/`, `query/`, `connection/`, `storage/`, `data/`, `config/`, `facade/`; all public re-exports preserved for backward compatibility
* UI layer reorganized: workspace, sidebar, status bar, and tasks panel moved into `views/`; modals into `overlays/`; toast and dropdown into `components/`
* `key_value.rs` (4601 lines) split into 9 focused submodules; `settings.rs` split into lifecycle/dirty_state/sidebar_nav; `connection_manager/render.rs` split into per-section files
* Documentation updated (ARCHITECTURE.md, AGENTS.md, CLAUDE.md, CODE_STYLE.md) to reflect new directory structure and `mod.rs` convention

---

## [0.4.0-dev.4] – 2026-03-04

### Added

* Proxy tunnel support: SOCKS5 and HTTP CONNECT proxy profiles with full CRUD in Settings, per-connection proxy selection in Connection Manager, and automatic tunnel lifecycle (RAII)
* New `dbflux_tunnel_core` crate extracting shared RAII tunnel lifecycle, `TunnelConnector` trait, `ForwardingConnection<R>` bidirectional forwarder, and adaptive sleep strategy
* Proxy profiles stored in `~/.config/dbflux/proxies.json` with keyring-backed secret storage for Basic auth passwords
* `no_proxy` pattern matching (curl/wget `NO_PROXY` semantics: wildcard, exact, suffix with/without leading dot)
* Proxy tab in Connection Manager with dropdown selector, read-only details, and "Edit in Settings" button
* Proxies section in Settings with full form CRUD, auth selection (None/Basic), enable/disable toggle, and keyboard navigation
* Guard preventing simultaneous proxy and SSH tunnel on the same connection

### Changed

* `JsonStore<T>` generic replaces three near-identical store structs (profiles, SSH tunnels, proxies)
* `ItemManager<T>` with `Identifiable` trait replaces duplicated `ProxyManager` and `SshTunnelManager` implementations
* `HasSecretRef` trait unifies secret operations across SSH tunnel and proxy profiles
* `FormGridNav<F>` generic extracts shared 2D grid navigation from proxy and SSH tunnel settings forms
* SSH tunnel forwarding loop now uses `ForwardingConnection<ssh2::Channel>` from `dbflux_tunnel_core` and gains adaptive sleep (50ms idle / 1ms active)
* SSH tunnel read-only mode in Connection Manager now shows saved tunnel details with "Edit in Settings" button
* Connection Manager tab order changed to Main → Settings → SSH → Proxy

### Fixed

* HTTP CONNECT proxy `BufReader` could silently consume post-handshake bytes; replaced with byte-by-byte `read_http_line()`
* Proxy tunnel handle is now preserved across database switches so the tunnel stays alive for the connection lifetime

---

## [0.4.0-dev.3] – 2026-03-03

### Added

* Connection hooks system: define reusable command hooks in Settings and bind them to connection profiles for execution during connect/disconnect phases (PreConnect, PostConnect, PreDisconnect, PostDisconnect)
* Each hook runs as an external process with command/args, optional cwd, custom env vars, timeout, and cancellation support
* Per-hook failure policy: Disconnect (abort flow), Warn (continue with warning), Ignore (log only)
* Hooks section in Settings with full CRUD for global hook definitions
* Hooks tab in Connection Manager with dropdown binding and extra IDs per phase
* Each hook executes as its own background task with stdout/stderr details visible in the Tasks panel (expandable with chevron toggle, 40-line truncation)
* Reusable `TreeNav` component for tree-based navigation with cursor movement, expand/collapse, select-by-id, and dedicated unit coverage
* New persisted UI state store (`UiStateStore`) in `~/.local/share/dbflux/state.json` to keep Settings category collapse state out of `config.json`
* 56 tests covering hook types, serde, execution, runner orchestration, binding resolution, and end-to-end integration

### Changed

* Driver resolution and command routing centralized behind shared core contracts, removing driver-specific conditionals from UI code
* Hook binding resolution moved from app layer to `dbflux_core` (`ConnectionHooks::resolve_from_bindings`) for testability
* Disconnect flow changed from synchronous to async to support pre/post-disconnect hooks
* Settings sidebar moved from a flat list to a tree with collapsible Network/Connection groups, persistent collapse state, and gutter connector lines
* Tree gutter rendering is now shared between Settings and Sidebar for consistent connector visuals and row sizing

### Fixed

* Sidebar profile/database chevron interactions now behave correctly: connected profiles expand/collapse, disconnected profiles connect on first click, and active profile changes stay in sync with tree state

---

## [0.4.0-dev.2] – 2026-03-01

### Added

* Settings now includes General, Services, and Drivers sections, with schema-backed per-driver override controls
* New Services settings section for configuring external RPC driver services
* Connection Manager now includes a Settings tab for per-connection overrides of global/driver policies and driver-owned settings

### Changed

* Driver settings now flow end-to-end through core types, AppState, and IPC contracts via `driver_key`-based resolution
* Driver capability declarations were audited, and the Drivers UI now shows capability chips filtered by database category
* Settings safety controls moved from toggle rows to explicit dropdowns, and theme changes now apply live

### Fixed

* False Drivers "Unsaved Changes" prompts were removed by switching dirty detection to deterministic value comparison against persisted settings
* Settings sidebar now provides a dedicated Close action that respects unsaved-change confirmation
* Save-password now defaults to enabled in the connection flow

---

## [0.4.0-dev.1] – 2026-02-28

### Added

* Comprehensive live integration tests for all five database drivers (43 tests covering schema introspection, CRUD, browse/count, explain, describe, cancellation, code generators, document CRUD, and KeyValueApi)
* Docker-based test infrastructure for PostgreSQL, MySQL, MongoDB, and Redis with automatic container lifecycle management
* Driver contract validation tests for metadata, form definitions, and capability declarations
* Explicit unsupported-value representation in query results (`UNSUPPORTED<type>`) to distinguish decode gaps from real `NULL` values
* Full PostgreSQL `tsvector` and `tsquery` support in data grid (browsing, query results, filtering)

### Changed

* AppState now accepts an external driver registry (`new_with_drivers`), making driver wiring controllable across different runtime contexts
* Document open and query connection selection extracted into explicit decision paths for consistent handling of missing connections and per-database routing
* MySQL `information_schema` queries migrated from `format!()` string interpolation to parameterized queries (`conn.exec` with `?` placeholders)
* MySQL nullable column reads (`Option<String>`) now use `row.get_opt()` to correctly distinguish SQL NULL from missing columns
* PostgreSQL custom type text decoding limited to safe textual types (enum/domain variants) to avoid silent mis-decoding
* Version bumped from `0.4.0-dev.0` to `0.4.0-dev.1`

### Fixed

* MySQL schema introspection panic on MySQL 8.4 where `column_key` in `information_schema.columns` can be NULL
* MySQL constraint introspection panic where `GROUP_CONCAT` over a `LEFT JOIN` returns NULL for CHECK constraints without key columns
* Windows portable builds no longer open a CMD console window when launched outside a terminal
* False startup exit when the app-control IPC check raced with window creation
* Filter submenu action dispatch repaired

---

## [0.4.0-dev.0] – 2026-02-26

### Added

* First development pre-release line for v0.4 on the `dev` branch
* IPC workspace crates integrated for local v0.4 testing (`dbflux_ipc`, `dbflux_driver_ipc`, `dbflux_driver_host`)

### Changed

* Project version bumped from `0.3.4` to `0.4.0-dev.0`
* Nix default package version bumped to `0.4.0-dev.0`

---

## [0.3.7] – 2026-03-06

### Added

* Dedicated style CI workflow that runs `cargo fmt --check` and `cargo clippy --workspace -- -D warnings`

### Changed

* New connections and SSH tunnels now save credentials to the system keyring by default unless the checkbox is explicitly disabled
* Release workflow now blocks artifact builds until both tests and style checks pass

### Fixed

* SQL query context selectors now refresh when connections and databases change, so tabs opened before connecting can pick their execution target correctly
* Per-database query task cancellation now cleans up the exact connection target, allowing the sidebar to reopen those databases after cancellation
* PostgreSQL connection retry setup and live integration tests now compile cleanly under current type inference requirements
* Restored the full `LICENSE-MIT` text in release artifacts

---

## [0.3.6] – 2026-02-28

### Added

* Comprehensive live integration tests for all five database drivers (43 tests covering schema introspection, CRUD, browse/count, explain, describe, cancellation, code generators, document CRUD, and KeyValueApi)
* Docker-based test infrastructure for PostgreSQL, MySQL, MongoDB, and Redis with automatic container lifecycle management
* Driver contract validation tests for metadata, form definitions, and capability declarations

### Changed

* AppState now accepts an external driver registry, making driver wiring controllable across different runtime contexts
* Document open and query connection selection extracted into explicit decision paths for consistent handling of missing connections and per-database routing
* MySQL `information_schema` queries migrated from `format!()` string interpolation to parameterized queries (`conn.exec` with `?` placeholders)
* MySQL nullable column reads (`Option<String>`) now use `row.get_opt()` to correctly distinguish SQL NULL from missing columns

### Fixed

* MySQL schema introspection panic on MySQL 8.4 where `column_key` in `information_schema.columns` can be NULL
* MySQL constraint introspection panic where `GROUP_CONCAT` over a `LEFT JOIN` returns NULL for CHECK constraints without key columns
* Windows portable builds no longer open a CMD console window when launched outside a terminal
* CI integration test job now installs required system dependencies (`libdbus-1-dev`, `libxkbcommon-dev`)

## [0.3.5] – 2026-02-26

### Added

* Explicit unsupported-value representation in query results (`UNSUPPORTED<type>`) to distinguish decode gaps from real `NULL` values

### Changed

* Unsupported values are now treated as read-only in the data grid and are excluded from save/copy mutation flows

### Fixed

* Added complete PostgreSQL `tsvector`/`tsquery` handling across table browse, query results, and grid filtering
* PostgreSQL fallback decode paths no longer misrepresent unknown types as `NULL`, reducing confusion and avoiding incorrect edits

---

## [0.3.4] – 2026-02-26

### Added

* Inline enum/set dropdown editing in the data grid with keyboard navigation (`j/k`, arrows, `Enter`, `Esc`)
* Nullable enum editing support with explicit `NULL` option in dropdowns
* Driver-level enum value metadata (`enum_values`) in `ColumnInfo` for PostgreSQL and MySQL
* Info-level logging for unsupported value decoding paths in PostgreSQL, MySQL, and SQLite drivers

### Changed

* PostgreSQL column introspection now uses `pg_catalog` + `format_type(...)` to preserve real type names (including user-defined types)
* PostgreSQL generated SQL literals now use escaped single-quoted string literals for readability
* MySQL `ENUM(...)` and `SET(...)` column definitions are parsed and exposed as selectable values in the UI

### Fixed

* PostgreSQL custom types (enum/domain/composite/range) no longer appear as `NULL` due to restrictive string decoding
* Table mode command routing now handles `Execute`/`Cancel` correctly, restoring keyboard-driven inline editing flow
* `LIKE` filter generation now only adds `ESCAPE '\\'` when required by the search value
* PostgreSQL `uuid` columns now cast to `::text` for `LIKE` filters

---

## [0.3.3] – 2026-02-26

### Added

* File-backed "New Tab" flow and keyboard navigation in the context bar
* Settings toggle to mask and reveal SSH password fields
* MongoDB sidebar metadata with collection-level indexes and a database-level indexes folder
* MongoDB field schema sampling in the sidebar (field type, optionality, nested fields)

### Changed

* Sidebar schema folders now stay visible with zero counts while lazy details load
* SQL and MongoDB schema folders are collapsed by default to avoid layout jumps during refresh

### Fixed

* Sidebar expansion no longer gets stuck in a loading state when opening schema nodes
* Closing a database connection no longer blocks the UI thread and freezes the app

---

## [0.3.2] – 2026-02-24

### Added

* Filter submenu in data grid context menu for SQL databases (=, <>, >, <, IS NULL, IS NOT NULL, Remove filter)
* Order submenu in data grid context menu for SQL databases (ASC, DESC, Remove order)
* MongoDB filter submenu in document tree context menu with Extended JSON values, `$and` composition, and NULL semantics (`$exists` guard)
* ListFilter and ArrowUpDown icons
* Empty state for the sidebar connections tab ("No connections yet" hint)

### Changed

* CI release workflow extracts changelog section from CHANGELOG.md instead of using hardcoded text

### Fixed

* GPUI newline panic: escape control characters in `Value::Json` preview in document tree
* GPUI newline panic: escape control characters in `Value::Text` and catch-all rendering in document card view
* GPUI newline panic: use compact JSON (no newlines) when composing MongoDB filters for the single-line filter input
* Toolbar clear-filter button now re-runs the query after clearing (was only calling `cx.notify()` without `refresh()`)
* Refactored icon asset loading to use `ALL_ICONS` lookup table instead of match arms

---

## [0.3.1] – 2026-02-24

### Fixed

* Table expansion in sidebar now loads and displays columns, indexes, and foreign keys instead of showing a stuck "Loading..." placeholder
* Concurrent table expansions no longer overwrite each other (replaced single pending action slot with per-item map)
* Failed schema fetches now collapse the table node instead of leaving it stuck in loading state
* Cache key mismatch between tree builder and fetch path that prevented details from ever appearing for per-database connections

### Added

* Collapsed sidebar now shows separate buttons for Connections and Scripts tabs
* FileCode icon registered in asset source

---

## [0.3.0] – 2026-02-23

### Added

#### MongoDB Support

* MongoDB driver with collection browsing, CRUD operations, and schema introspection
* Document tree view with keyboard navigation, search, and value expansion
* MongoDB query parsing and validation with positional diagnostics
* MongoDB shell query generator for "Copy as Query" support
* Document view context menu with language-aware editor

#### Redis Support

* Redis driver with key-value API integration
* Key-value document browser with keyboard-navigable new-key modal
* Support for all Redis data types: String, Hash, Set, Sorted Set, List, Stream
* Context menu and real pagination for the key browser
* Live TTL countdown display
* Add Member modal for collection types
* Redis key completions and command arity validation in the editor

#### Script Documents

* File-backed query documents with Open (`Ctrl+O`), Save (`Ctrl+S`), and Save As (`Ctrl+Shift+S`)
* Execution context bar with connection, database, and schema dropdowns per tab
* Scripts folder in the sidebar with file and folder management

#### Session Persistence

* Auto-save on a 2-second debounce after each keystroke
* Scratch files for untitled tabs, shadow files for file-backed tabs (explicit `Ctrl+S` still writes the original)
* Full session restore on startup from `~/.local/share/dbflux/sessions/`
* Conflict detection: warns when original file was modified externally while a shadow existed
* Tabs close without unsaved-changes warnings

#### Per-Database Connections

* PostgreSQL supports multiple databases open simultaneously in the sidebar
* Query tabs target a specific database connection instead of sharing a single switchable one

#### Document System

* Tab-based document architecture with `DocumentHandle` and `TabManager`
* SQL query documents with multiple result tabs (MRU ordering)
* Collapsible, resizable sidebar dock and bottom dock panels
* History modal integrated with document-based focus system

#### Editor Enhancements

* Language-aware autocompletion (SQL tables/columns, MongoDB collections, Redis keys)
* Live query diagnostics with positional error markers
* Redis command arity validation in the editor

#### Data Grid

* Inline cell editing with focus handling
* Modal editor for JSON and long text values (`CellEditorModal`)
* Context menu with CRUD operations and SQL generation
* Keyboard navigation in context menus
* Column resizing via drag
* Support for empty tables in the data grid
* Row insert and duplicate without requiring a primary key

#### Query Generation

* Unified query generation with "Copy as Query" and preview modal
* `QueryGenerator` trait implemented by PostgreSQL, MySQL, SQLite, MongoDB, and Redis drivers
* `SqlDialect` trait for SQL flavor differences across drivers

#### Export

* Multi-format export: CSV, JSON, Text, and Binary
* Export generalized by result shape instead of hardcoded CSV

#### Auto-Refresh

* Interval-based auto-refresh with unified refresh split button
* `DocumentTaskRunner` for unified async task tracking

#### Connection Manager

* URI connection mode for PostgreSQL and MySQL
* Bidirectional sync between connection URI and individual form fields

#### Query Safety

* Dangerous query detection for SQL, MongoDB, and Redis commands
* Confirmation dialog with query preview before destructive operations

#### Sidebar

* Schema-level indexes, foreign keys, and data types in the tree
* Schema-level metadata support for MySQL and SQLite
* Context menus for indexes, foreign keys, and custom types
* `q`/`e` keys to switch between Connections and Scripts tabs
* Inline rename in the tree (both tabs)
* Default focus to sidebar on startup when no tabs are open

#### Packaging & CI

* macOS release builds with `.app` bundle (`Info.plist`)
* Windows release builds with Inno Setup installer
* MongoDB and Redis feature flags enabled in default builds

### Changed

* `CellValue` pre-computes display text at construction time (avoids allocation during render)
* Lazy loading for PostgreSQL and SQLite drivers (shallow metadata first, details on demand)
* Sidebar uses `SchemaNodeId` parsing instead of stale underscore prefixes
* Custom toast implementation replaces `gpui-component` toast
* AppState decomposed into focused sub-managers in `dbflux_core`
* Architecture decoupled: core traits, driver capabilities, and error formatting extracted
* Oversized UI modules split into focused submodules (sidebar, SQL query, modals, SSH form)
* Active context detection improved in data grids
* Document focus restored correctly across menus and modals
* Scripts tab styling matches connections tab (icon and label colors)
* Removed force-close flow (double `Ctrl+W` warning, pending force close state)

### Performance

* Fixed catastrophic 1 FPS rendering issue in the data table
* Row-level event handlers replace per-cell closures in tables
* Background executor used consistently for all database operations

### Fixed

* Document focus restored across menus and modals
* Redis database state handling and UI interaction bugs
* SSH tunnel form mouse focus syncs with keyboard state
* Settings sync between SSH form fields
* Panics and unwraps eliminated across UI and driver code
* Empty query results now return column metadata correctly
* DDL queries show preview modal and editor height is correct
* Sidebar "New File"/"New Folder" creates inside the selected folder instead of at root
* Reveal in File Manager works on macOS and Windows (not just Linux)
* Opening an already-open script activates its tab instead of closing it

## [0.2.0] – 2026-01-30

### Added

#### MySQL Support

* MySQL/MariaDB driver with full query execution and schema introspection
* Dual connection architecture (sync for schema, async for queries)
* Dynamic connection forms that adapt to driver-specific requirements

#### Sidebar Enhancements

* Folder-based organization for connection profiles
* Drag and drop support for connections and folders
* Multi-selection (Shift+click, Ctrl+click)
* Keyboard shortcuts for rename, delete, and new folder actions

#### Query Safety

* Confirmation dialogs for dangerous SQL queries (DELETE, DROP, TRUNCATE without WHERE)
* Driver-delegated SQL generation from context menu (SELECT, INSERT, UPDATE, DELETE)

#### Results Table

* Column sorting via header clicks (ASC/DESC)
* Custom DataTable component with virtualized rendering

#### Icons

* Centralized SVG icon system with `AppIcon` enum and compile-time embedding
* Icons across the editor toolbar (History, Save), Run/Cancel buttons, and tabs
* Sidebar tree icons (database brands, folders, tables, views, columns, indexes)
* Icons in context menus, results footer, pagination, and export actions
* Icons in connection manager (tabs, form headers, buttons)
* Icons in settings sidebar and About section
* Icons in toast notifications (success, info, warning, error)
* Icons in confirmation dialogs (delete, dangerous query)
* Database brand icons for PostgreSQL, MySQL, MariaDB, and SQLite
* Third-party licenses listed in About (Lucide ISC, Simple Icons CC0)

#### Packaging & Distribution

* Nix flake with development shell
* Arch Linux PKGBUILD
* Linux installer script (`curl | bash`)
* GPG-signed release artifacts
* GitHub Actions–based release workflow

### Changed

* Lazy loading of table details in the sidebar (improves performance on large schemas)
* Schema loading deferred until node expansion
* Active databases are now visually highlighted in the sidebar

### Performance

* Eliminated hover-induced re-renders in the data table
* Fixed subscription leaks in the table component

### Fixed

* Horizontal auto-scroll when navigating the data table with the keyboard

## [0.1.2] - 2025-01-25

### Fixed

- Connection Manager: SQLite form navigation now works correctly (`j/k` navigates between Name, File Path, and action buttons instead of jumping to non-existent PostgreSQL fields)
- Connection Manager: Pressing Enter while editing an input now exits edit mode and moves to the next field
- Connection Manager: Input blur events now properly restore keyboard navigation focus

## [0.1.1] - 2025-01-25

### Added

- About section in Settings with version info, GitHub links, and license (Apache 2.0 / MIT)
- SSH tunnel form keyboard navigation (row-based: `j/k` between rows, `h/l` within fields, `Tab` sequential, `g/G` first/last)
- Database switch now appears as cancellable background task

### Fixed

- Settings window now opens as singleton (reuses existing window instead of opening duplicates)
- Stale settings window handle is now cleared when the window is closed
- SSH form field selection resets to valid field when switching auth method (PrivateKey ↔ Password)
- SSH selected index adjusts correctly when tunnels are deleted
- `z` keybinding for panel collapse now works in Editor and Background Tasks (previously only Results)

## [0.1.0] - 2025-01-25

Initial release of DBFlux.

### Added

#### Database Support
- PostgreSQL driver with full query execution and schema introspection
- SQLite driver for local database files
- SSL/TLS support for PostgreSQL (Disable, Prefer, Require modes)
- SSH tunnel support with multiple authentication methods (key, password, agent)
- Reusable SSH tunnel profiles

#### User Interface
- Three-panel workspace layout (Sidebar, Editor, Results)
- Resizable and collapsible panels
- Schema tree browser with hierarchical navigation (databases, schemas, tables, views, columns, indexes)
- Visual indicators for column properties (primary key, nullable, type)
- Multi-tab SQL editor with syntax highlighting
- Virtualized results table with column resizing
- Table browser mode with WHERE filters, custom LIMIT, and pagination
- Command palette with fuzzy search and scroll support
- Toast notifications for user feedback
- Background tasks panel with progress and cancellation
- Status bar showing connection and task status
- Keyboard-navigable context menus with nested submenu support

#### SQL Execution
- Query execution with result display
- Query cancellation support (PostgreSQL uses `pg_cancel_backend`, SQLite uses `sqlite3_interrupt`)
- Execution time and row count display
- Multiple result tabs

#### Query Management
- Query history with timestamps and execution metadata
- Saved queries with favorites support
- Search and filter across history and saved queries
- Unified history/saved queries modal with keyboard navigation
- Persistent storage in `~/.config/dbflux/`

#### Connection Management
- Connection profiles with secure password storage (system keyring)
- Connection manager with full form validation
- Test connection before saving
- Quick connect/disconnect from sidebar

#### Keyboard Navigation
- Vim-style navigation (j/k/h/l) throughout the application
- Context-aware keybindings (Sidebar, Editor, Results, History, Settings)
- Global shortcuts for common actions
- Tab cycling between panels
- Full keyboard support in connection manager form
- Results toolbar navigation: `f` to focus toolbar, `h/l` to navigate elements, `Enter` to edit/execute, `Esc` to exit
- Panel collapse toggle with `z` key
- Context menu navigation: `j/k` to move, `Enter` to select, `l` to open submenu, `h/Esc` to close

#### Export
- CSV export for query results

#### Settings
- SSH tunnel profile management
- Keybindings reference section with collapsible context groups and search filter

### Known Limitations

- No dark/light theme toggle (uses system default)
