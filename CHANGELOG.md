# Changelog

All notable changes to DBFlux will be documented in this file.

## [0.4.0-dev.1] – 2026-02-28

_Placeholder – will be filled after reviewing changes._

---

## [0.4.0-dev.0] – 2026-02-26

### Added

* First development pre-release line for v0.4 on the `dev` branch
* IPC workspace crates integrated for local v0.4 testing (`dbflux_ipc`, `dbflux_driver_ipc`, `dbflux_driver_host`)

### Changed

* Project version bumped from `0.3.4` to `0.4.0-dev.0`
* Nix default package version bumped to `0.4.0-dev.0`

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
