# DBFlux

A fast, keyboard-first database client built with Rust and GPUI.

## Overview

DBFlux is an open-source database client written in Rust, built with GPUI (Zed's UI framework). It focuses on performance, a clean UX, and keyboard-first workflows.

The long-term goal is to provide a fully open-source alternative to DBeaver, supporting both relational and non-relational databases.

## Installation

### Quick Install (Linux)

```bash
# Install to /usr/local (requires sudo)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | sudo bash

# Install to ~/.local (no sudo required)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --prefix ~/.local
```

### Build from Source

```bash
# Via install script
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --build

# Or manually
git clone https://github.com/0xErwin1/dbflux.git
cd dbflux
cargo build --release --features sqlite,postgres,mysql
./target/release/dbflux
```

### Arch Linux

Using the provided PKGBUILD:

```bash
cd scripts
makepkg -si
```

Or with an AUR helper (once published):

```bash
paru -S dbflux
```

### Nix

Using flakes:

```bash
# Run directly
nix run github:0xErwin1/dbflux

# Install to profile
nix profile install github:0xErwin1/dbflux

# Development shell
nix develop github:0xErwin1/dbflux
```

Or with the traditional approach:

```bash
nix-build
./result/bin/dbflux
```

### Uninstall

```bash
# If installed with install.sh
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | sudo bash

# From ~/.local
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | bash -s -- --prefix ~/.local

# Remove user config and data too
./scripts/uninstall.sh --remove-config
```

### Verify Downloads

All release artifacts are signed with GPG (key `A614B7D25134987A`).

```bash
# Import the public key from keyserver (one time)
gpg --keyserver keyserver.ubuntu.com --recv-keys A614B7D25134987A

# Verify checksum
sha256sum -c dbflux-linux-amd64.tar.gz.sha256

# Verify GPG signature
gpg --verify dbflux-linux-amd64.tar.gz.asc dbflux-linux-amd64.tar.gz
```

## Features

### Database Support

- **PostgreSQL** with SSL/TLS modes (Disable, Prefer, Require)
- **MySQL** / MariaDB
- **SQLite** for local database files
- SSH tunnel support with key, password, and agent authentication
- Reusable SSH tunnel profiles

### User Interface

- Document-based workspace with multiple result tabs (like DBeaver/VS Code)
- Collapsible, resizable sidebar with ToggleSidebar command (Ctrl+B)
- Schema tree browser with lazy loading for large databases
- Schema-level metadata: indexes, foreign keys, constraints, custom types (PostgreSQL)
- Multi-tab SQL editor with syntax highlighting
- Virtualized data table with column resizing, horizontal scrolling, and sorting
- Table browser with WHERE filters, custom LIMIT, and pagination
- Command palette with fuzzy search
- Custom toast notification system with auto-dismiss
- Background task panel

### Keyboard Navigation

- Vim-style navigation (`j`/`k`/`h`/`l`) throughout the app
- Context-aware keybindings (Document, Sidebar, BackgroundTasks)
- Document focus with internal editor/results navigation
- Results toolbar: `f` to focus, `h`/`l` to navigate, `Enter` to edit/execute, `Esc` to exit
- Toggle sidebar with `Ctrl+B`
- Tab switching (MRU order) with `Ctrl+Tab` / `Ctrl+Shift+Tab`
- History modal: `Ctrl+P` to open

### Query Management

- Query history with timestamps
- Saved queries with favorites
- Search across history and saved queries

### Export

- CSV export for query results

## Development

### Prerequisites

**Ubuntu/Debian:**
```bash
sudo apt install pkg-config libssl-dev libdbus-1-dev libxkbcommon-dev
```

**Fedora:**
```bash
sudo dnf install pkg-config openssl-devel dbus-devel libxkbcommon-devel
```

**Arch:**
```bash
sudo pacman -S pkg-config openssl dbus libxkbcommon
```

### Building

```bash
cargo build -p dbflux --release --features sqlite,postgres,mysql
```

### Running

```bash
cargo run -p dbflux --features sqlite,postgres,mysql
```

### Commands

```bash
cargo check --workspace                    # Type checking
cargo clippy --workspace -- -D warnings    # Lint
cargo fmt --all                            # Format
cargo test --workspace                     # Tests
```

### Nix Development Shell

If you use Nix, you can enter a development shell with all dependencies:

```bash
# With flakes
nix develop

# Traditional
nix-shell
```

## Project Structure

```
dbflux/
├── crates/
│   ├── dbflux/                 # Main application
│   │   ├── ui/
│   │   │   ├── document/       # Document system (SqlQuery, DataDocument)
│   │   │   ├── dock/           # SidebarDock, BottomDock
│   │   │   ├── components/     # DataTable, icons
│   │   │   └── ...             # Other UI panels
│   │   └── keymap/             # Keyboard system
│   ├── dbflux_core/            # Core types and traits
│   ├── dbflux_driver_sqlite/   # SQLite driver
│   ├── dbflux_driver_postgres/ # PostgreSQL driver
│   ├── dbflux_driver_mysql/    # MySQL driver
│   ├── dbflux_ssh/             # SSH tunnel support
│   └── dbflux_export/          # Export functionality
├── resources/
│   ├── desktop/                # Desktop entry
│   ├── icons/                  # Application icons
│   └── mime/                   # MIME type definitions
└── scripts/
    ├── install.sh              # Linux installer
    ├── uninstall.sh            # Linux uninstaller
    └── PKGBUILD                # Arch Linux package
```

## License

MIT & Apache-2.0
