# Changelog

All notable changes to DBFlux will be documented in this file.

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

- Export limited to CSV format (JSON, SQL, Excel planned)
- No query autocompletion
- No dark/light theme toggle (uses system default)
