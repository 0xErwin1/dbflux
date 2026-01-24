# Architecture

## Overview
- DBFlux is a keyboard-first database client built with Rust and GPUI, focused on fast workflows and a clean desktop UI (README.md).
- The repo is a Rust workspace with a UI app crate plus shared core types, driver implementations, and supporting libraries (Cargo.toml, crates/).

## Tech Stack
- Language: Rust 2024 edition (crates/dbflux/Cargo.toml).
- UI: `gpui`, `gpui-component` (Cargo.toml).
- Databases: `tokio-postgres` (PostgreSQL), `rusqlite` (SQLite) (Cargo.toml).
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
    src/keymap/             # Keyboard commands and keymap
  dbflux_core/              # Traits, core types, storage, errors
    src/traits.rs           # DbDriver + Connection traits
    src/profile.rs          # Connection/SSH profiles
    src/store.rs            # Profile and tunnel stores (JSON)
    src/history.rs          # History persistence
    src/task.rs             # Background task tracking
  dbflux_driver_postgres/   # PostgreSQL driver implementation
  dbflux_driver_sqlite/     # SQLite driver implementation
  dbflux_ssh/               # SSH tunnel support
  dbflux_export/            # CSV export
```

## Core Components
- App entry point: `crates/dbflux/src/main.rs` initializes logging, theme, and main GPUI window.
- Global app state: `crates/dbflux/src/app.rs` owns drivers, profiles, active connections, history, task manager, and secret store access.
- Workspace UI shell: `crates/dbflux/src/ui/workspace.rs` wires panes (sidebar/editor/results/tasks), command palette, and focus routing.
- Core domain API: `crates/dbflux_core/src/traits.rs` defines `DbDriver`, `Connection`, and cancellation contracts.
- Profiles + secrets: `crates/dbflux_core/src/profile.rs` and `crates/dbflux_core/src/secrets.rs` define connection/SSH profiles and keyring integration.
- Storage: `crates/dbflux_core/src/store.rs` and `crates/dbflux_core/src/history.rs` persist JSON data in the config dir.
- Drivers: `crates/dbflux_driver_postgres/src/driver.rs` and `crates/dbflux_driver_sqlite/src/driver.rs` implement query execution + schema discovery.
- SSH tunneling: `crates/dbflux_ssh/src/lib.rs` establishes SSH sessions and runs a local port forwarder.
- Export: `crates/dbflux_export/src/lib.rs` exposes the CSV exporter interface.

## Data Flow
- Startup: `main` creates `AppState` and `Workspace`, then opens the main window (crates/dbflux/src/main.rs).
- Connect flow: `AppState::prepare_connect_profile` selects a driver and builds `ConnectProfileParams`, which connects and fetches schema (crates/dbflux/src/app.rs).
- Query flow: Editor pane submits SQL to a `Connection` implementation; results are rendered in `ResultsPane` (crates/dbflux/src/ui/editor/mod.rs, crates/dbflux/src/ui/results/mod.rs).
- Schema refresh: `Workspace::refresh_schema` runs `Connection::schema` on a background executor and updates `AppState` (crates/dbflux/src/ui/workspace.rs).
- History flow: completed queries are stored in `HistoryStore`, persisted to JSON, and exposed in the sidebar (crates/dbflux_core/src/history.rs).

## External Integrations
- PostgreSQL: `tokio-postgres` client with optional TLS and cancellation support (crates/dbflux_driver_postgres/src/driver.rs).
- SQLite: `rusqlite` file-based connections (crates/dbflux_driver_sqlite/src/driver.rs).
- SSH: `ssh2` sessions with local TCP forwarding (crates/dbflux_ssh/src/lib.rs).
- OS keyring: optional secret storage for passwords and SSH passphrases (crates/dbflux_core/src/secrets.rs).
- CSV export: `csv::Writer` for result exports (crates/dbflux_export/src/csv.rs).

## Configuration
- Workspace settings: `Cargo.toml` defines workspace members and shared dependencies.
- App features: `crates/dbflux/Cargo.toml` gates `sqlite` and `postgres` drivers.
- Runtime data (config dir via `dirs::config_dir`):
  - `profiles.json` and `ssh_tunnels.json` (crates/dbflux_core/src/store.rs).
  - Query history JSON (crates/dbflux_core/src/history.rs).
- Secrets: passwords stored in OS keyring; references derived from profile IDs (crates/dbflux_core/src/secrets.rs).

## Build & Deploy
- Build: `cargo build -p dbflux` or `cargo build -p dbflux --release` (AGENTS.md).
- Run: `cargo run -p dbflux` (AGENTS.md).
- Test: `cargo test --workspace` (AGENTS.md).
- Lint/format: `cargo clippy --workspace -- -D warnings`, `cargo fmt --all` (AGENTS.md).
- Deployment model: desktop GUI app; no server runtime in this repo.
