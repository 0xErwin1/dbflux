# DBFlux

A fast, keyboard-first database client built with Rust and GPUI.

## Overview

DBFlux is an open-source database client written in Rust, built with GPUI (Zed's UI framework). It focuses on performance, a clean UX, and keyboard-first workflows.

The long-term goal is to provide a fully open-source alternative to DBeaver, supporting both relational and non-relational databases.

## Installation

### Linux

#### Tarball (recommended)

```bash
# Install to /usr/local (requires sudo)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | sudo bash

# Install to ~/.local (no sudo required)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --prefix ~/.local
```

#### AppImage (portable)

```bash
# Download from releases (replace amd64 with arm64 for ARM)
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.AppImage
chmod +x dbflux-linux-amd64.AppImage
./dbflux-linux-amd64.AppImage
```

#### Arch Linux

Using the provided PKGBUILD:

```bash
cd scripts
makepkg -si
```

Or with an AUR helper (once published):

```bash
paru -S dbflux
```

#### Nix

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

### macOS

DBFlux for macOS is not signed with an Apple developer certificate. When opening for the first time, you'll see a warning about an "unidentified developer".

#### Installation

1. Download the DMG for your architecture from [Releases](https://github.com/0xErwin1/dbflux/releases):
   - **Intel Macs**: `dbflux-macos-amd64.dmg`
   - **Apple Silicon (M1/M2/M3/M4)**: `dbflux-macos-arm64.dmg`
2. Open the DMG and drag DBFlux to Applications
3. When you see the "unidentified developer" warning:
   - Go to **System Settings → Privacy & Security**
   - Click **Open Anyway** next to the security warning
   - Confirm you want to open the application

#### Bypass Gatekeeper from Terminal

```bash
# Remove quarantine attribute (allows opening without GUI confirmation)
xattr -cr /Applications/DBFlux.app

# Now you can open it normally
open /Applications/DBFlux.app
```

#### Requirements

- macOS 11.0 (Big Sur) or later

### Windows

#### Installer

1. Download `dbflux-windows-amd64-setup.exe` from [Releases](https://github.com/0xErwin1/dbflux/releases)
2. Run the installer and follow the wizard

#### Portable

1. Download `dbflux-windows-amd64.zip` from [Releases](https://github.com/0xErwin1/dbflux/releases)
2. Extract to any folder
3. Run `dbflux.exe`

> **Note**: The executable is not signed with a Windows code signing certificate. Windows SmartScreen may show a warning. Click "More info" → "Run anyway" to proceed.

#### Requirements

- Windows 10 or later
- x86_64 (ARM64 not yet supported)

### Build from Source

```bash
# Via install script (Linux)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --build

# Or manually
git clone https://github.com/0xErwin1/dbflux.git
cd dbflux
cargo build --release --features sqlite,postgres,mysql,mongodb
./target/release/dbflux
```

### Uninstall (Linux)

```bash
# If installed with install.sh
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | sudo bash

# From ~/.local
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | bash -s -- --prefix ~/.local

# Remove user config and data too
./scripts/uninstall.sh --remove-config
```

### Verify Downloads

Releases triggered with `workflow_dispatch` and `sign=true` include GPG signatures (key `A614B7D25134987A`).

```bash
# Import the public key from keyserver (one time)
gpg --keyserver keyserver.ubuntu.com --recv-keys A614B7D25134987A

# Verify checksum
sha256sum -c dbflux-linux-amd64.tar.gz.sha256

# Verify GPG signature (if the release includes .asc files)
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

**macOS:**
```bash
# Xcode Command Line Tools (required)
xcode-select --install
```

**Windows:**
```powershell
# Visual Studio Build Tools with C++ workload (required)
# Download from: https://visualstudio.microsoft.com/visual-cpp-build-tools/
```

### Building

```bash
cargo build -p dbflux --release --features sqlite,postgres,mysql,mongodb
```

### Running

```bash
cargo run -p dbflux --features sqlite,postgres,mysql,mongodb
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
│   ├── dbflux_driver_mongodb/  # MongoDB driver
│   ├── dbflux_ssh/             # SSH tunnel support
│   └── dbflux_export/          # Export functionality
├── resources/
│   ├── desktop/                # Linux desktop entry
│   ├── icons/                  # Application icons (SVG)
│   ├── macos/                  # macOS bundle resources
│   ├── mime/                   # MIME type definitions
│   └── windows/                # Windows installer resources
└── scripts/
    ├── install.sh              # Linux installer
    ├── uninstall.sh            # Linux uninstaller
    └── PKGBUILD                # Arch Linux package
```

## License

MIT & Apache-2.0
