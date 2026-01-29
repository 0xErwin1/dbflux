# Architecture

## Overview
- DBFlux is a keyboard-first database client built with Rust and GPUI, focused on fast workflows and a clean desktop UI (README.md).
- The repo is a Rust workspace with a UI app crate plus shared core types, driver implementations, and supporting libraries (Cargo.toml, crates/).

## Tech Stack
- Language: Rust 2024 edition (crates/dbflux/Cargo.toml).
- UI: `gpui`, `gpui-component` (Cargo.toml).
- Databases: `tokio-postgres` (PostgreSQL), `rusqlite` (SQLite), `mysql` (MySQL/MariaDB) (Cargo.toml).
- SSH: `ssh2` via `dbflux_ssh` (crates/dbflux_ssh/src/lib.rs).
- Export: `csv` + `hex` via `dbflux_export` (crates/dbflux_export/src/lib.rs).
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
    src/ui/editor.rs        # SQL editor with dangerous query detection
    src/ui/editor/dangerous_query.rs  # Query safety analysis
    src/ui/results.rs       # Results table with toolbar focus management
    src/ui/components/data_table/  # Custom virtualized data table
    src/ui/history_modal.rs # Recent/saved queries modal
    src/keymap/             # Keyboard system
    src/keymap/command.rs   # Command enum with all app actions
    src/keymap/defaults.rs  # Context-aware key bindings
    src/keymap/focus.rs     # FocusTarget enum for panel routing
  dbflux_core/              # Traits, core types, storage, errors
    src/traits.rs           # DbDriver + Connection traits
    src/driver_form.rs      # Dynamic form definitions per driver
    src/profile.rs          # Connection/SSH profiles
    src/connection_tree.rs  # Folder/connection tree model
    src/connection_tree_store.rs  # Tree persistence (JSON)
    src/store.rs            # Profile and tunnel stores (JSON)
    src/history.rs          # History persistence
    src/saved_query.rs      # Saved queries persistence
    src/task.rs             # Background task tracking
  dbflux_driver_postgres/   # PostgreSQL driver implementation
  dbflux_driver_sqlite/     # SQLite driver implementation
  dbflux_driver_mysql/      # MySQL/MariaDB driver implementation
  dbflux_ssh/               # SSH tunnel support
  dbflux_export/            # CSV export
```

## Core Components
- App entry point: `crates/dbflux/src/main.rs` initializes logging, theme, and main GPUI window.
- Global app state: `crates/dbflux/src/app.rs` owns drivers, profiles, active connections, history, task manager, and secret store access.
- Workspace UI shell: `crates/dbflux/src/ui/workspace.rs` wires panes (sidebar/editor/results/tasks), command palette, and focus routing.
- Sidebar: `crates/dbflux/src/ui/sidebar.rs` displays connection tree with folder organization, drag-drop reordering, multi-selection, and schema browser.
- Connection tree: `crates/dbflux_core/src/connection_tree.rs` models folders and connections as a tree structure with persistence via `connection_tree_store.rs`.
- Core domain API: `crates/dbflux_core/src/traits.rs` defines `DbDriver`, `Connection`, SQL generation, and cancellation contracts.
- Driver forms: `crates/dbflux_core/src/driver_form.rs` defines dynamic form schemas that drivers provide for connection configuration.
- Profiles + secrets: `crates/dbflux_core/src/profile.rs` and `crates/dbflux_core/src/secrets.rs` define connection/SSH profiles and keyring integration.
- Storage: `crates/dbflux_core/src/store.rs`, `crates/dbflux_core/src/history.rs`, and `crates/dbflux_core/src/saved_query.rs` persist JSON data in the config dir.
- History modal: `crates/dbflux/src/ui/history_modal.rs` provides a unified modal for browsing recent queries and saved queries with search, favorites, and rename support.
- Data table: `crates/dbflux/src/ui/components/data_table/` custom virtualized table with sorting, selection, and keyboard navigation.
- Query safety: `crates/dbflux/src/ui/editor/dangerous_query.rs` detects dangerous queries (DELETE/DROP/TRUNCATE without WHERE) and prompts for confirmation.
- Drivers: `crates/dbflux_driver_postgres/`, `crates/dbflux_driver_sqlite/`, and `crates/dbflux_driver_mysql/` implement query execution, schema discovery, and SQL generation.
- SSH tunneling: `crates/dbflux_ssh/src/lib.rs` establishes SSH sessions and runs a local port forwarder.
- Export: `crates/dbflux_export/src/lib.rs` exposes the CSV exporter interface.

## Data Flow
- Startup: `main` creates `AppState` and `Workspace`, then opens the main window (crates/dbflux/src/main.rs).
- Connect flow: `AppState::prepare_connect_profile` selects a driver and builds `ConnectProfileParams`, which connects and fetches schema (crates/dbflux/src/app.rs).
- Query flow: Editor pane submits SQL to a `Connection` implementation; results are rendered in `ResultsPane` (crates/dbflux/src/ui/editor.rs, crates/dbflux/src/ui/results.rs).
- Schema refresh: `Workspace::refresh_schema` runs `Connection::schema` on a background executor and updates `AppState` (crates/dbflux/src/ui/workspace.rs).
- History flow: completed queries are stored in `HistoryStore`, persisted to JSON, and accessible via the history modal (crates/dbflux_core/src/history.rs).
- Saved queries flow: users can save queries with names via `SavedQueryStore`; the history modal (Ctrl+P) allows browsing, searching, and loading saved queries (crates/dbflux_core/src/saved_query.rs).

## Keyboard & Focus Architecture
- Keymap system: `crates/dbflux/src/keymap/` defines `Command` enum, `KeyChord` parsing, context-aware `KeymapLayer` bindings, and `FocusTarget` for panel routing.
- Command dispatch: `Workspace` implements `CommandDispatcher` trait; `dispatch()` routes commands based on `focus_target` (Sidebar, Editor, Results, BackgroundTasks).
- Focus layers: Each context (sidebar, editor, results) has its own keymap layer with vim-style bindings (j/k/h/l navigation).
- Panel focus modes: Complex panels like Results have internal focus state machines (`FocusMode::Table`/`Toolbar`, `EditState::Navigating`/`Editing`) to handle nested keyboard navigation.
- Mouse/keyboard sync: Mouse handlers update focus state to keep keyboard and mouse navigation consistent; a `switching_input` flag prevents race conditions during input blur events.

## External Integrations
- PostgreSQL: `tokio-postgres` client with optional TLS and cancellation support (crates/dbflux_driver_postgres/src/driver.rs).
- MySQL/MariaDB: `mysql` crate with dual connection architecture (sync for schema, async for queries) (crates/dbflux_driver_mysql/src/driver.rs).
- SQLite: `rusqlite` file-based connections (crates/dbflux_driver_sqlite/src/driver.rs).
- SSH: `ssh2` sessions with local TCP forwarding (crates/dbflux_ssh/src/lib.rs).
- OS keyring: optional secret storage for passwords and SSH passphrases (crates/dbflux_core/src/secrets.rs).
- CSV export: `csv::Writer` for result exports (crates/dbflux_export/src/csv.rs).

## Configuration
- Workspace settings: `Cargo.toml` defines workspace members and shared dependencies.
- App features: `crates/dbflux/Cargo.toml` gates `sqlite` and `postgres` drivers.
- Runtime data (config dir via `dirs::config_dir`):
  - `profiles.json` and `ssh_tunnels.json` (crates/dbflux_core/src/store.rs).
  - `history.json` for query history (crates/dbflux_core/src/history.rs).
  - `saved_queries.json` for user-saved queries (crates/dbflux_core/src/saved_query.rs).
- Secrets: passwords stored in OS keyring; references derived from profile IDs (crates/dbflux_core/src/secrets.rs).

## Build & Deploy
- Build: `cargo build -p dbflux --features sqlite,postgres,mysql` or `--release` (AGENTS.md).
- Run: `cargo run -p dbflux --features sqlite,postgres,mysql` (AGENTS.md).
- Test: `cargo test --workspace` (AGENTS.md).
- Lint/format: `cargo clippy --workspace -- -D warnings`, `cargo fmt --all` (AGENTS.md).
- Nix: `nix build` or `nix run` using flake.nix; `nix develop` for dev shell.
- Arch Linux: `makepkg -si` using scripts/PKGBUILD.
- Linux installer: `curl -fsSL .../install.sh | bash` downloads and installs release.
- Releases: GitHub Actions workflow builds Linux amd64/arm64, signs with GPG, publishes to GitHub Releases.
- Deployment model: desktop GUI app; no server runtime in this repo.
