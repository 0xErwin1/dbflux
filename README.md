# DBFlux

A fast, keyboard-first database client built with Rust and GPUI.

## Overview

DBFlux is an open-source database client written in Rust, built with GPUI (Zed's UI framework). It focuses on performance, a clean UX, and keyboard-first workflows.

The long-term goal is to provide a fully open-source alternative to DBeaver, supporting both relational and non-relational databases.

## Features

### Database Support
- **PostgreSQL** with SSL/TLS modes (Disable, Prefer, Require)
- **SQLite** for local database files
- SSH tunnel support with key, password, and agent authentication
- Reusable SSH tunnel profiles

### User Interface
- Three-panel workspace (Sidebar, Editor, Results)
- Resizable and collapsible panels
- Schema tree browser with tables, views, columns, and indexes
- Multi-tab SQL editor
- Virtualized results table with column resizing
- Table browser with WHERE filters, custom LIMIT, and pagination
- Command palette with fuzzy search
- Toast notifications and background task panel

### Keyboard Navigation
- Vim-style navigation (`j`/`k`/`h`/`l`) throughout the app
- Context-aware keybindings per panel
- Results toolbar: `f` to focus, `h`/`l` to navigate, `Enter` to edit/execute, `Esc` to exit
- Panel collapse with `z`
- Tab cycling between panels

### Query Management
- Query history with timestamps
- Saved queries with favorites
- Search across history and saved queries

### Export
- CSV export for query results

## Building

```bash
cargo build -p dbflux --release
```

## Running

```bash
cargo run -p dbflux
```

## Development

```bash
cargo check --workspace              # Type checking
cargo clippy --workspace -- -D warnings  # Lint
cargo fmt --all                      # Format
cargo test --workspace               # Tests
```

## License

MIT & Apache-2.0
