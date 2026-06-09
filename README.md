# DBFlux

A fast, keyboard-first database client built with Rust and GPUI.

## Overview

DBFlux is an open-source database client written in Rust, built with GPUI (Zed's UI framework). It focuses on performance, a clean UX, and keyboard-first workflows.

The long-term goal is to provide a fully open-source alternative to DBeaver, supporting both relational and non-relational databases.

![DBFlux](resources/dbflux.png)

## Documentation

### User guides

- [Usage Guide](docs/USAGE.md) — getting started: connect, query, chart, export
- [Connecting — Advanced Setup](docs/CONNECTIONS.md) — SSH tunnels, proxies, AWS SSO auth profiles, value sources
- [Settings & Hooks](docs/SETTINGS.md) — every Settings section and connection hooks
- [Data & Privacy](docs/DATA_AND_PRIVACY.md) — where your data and secrets live, backup and reset
- [Dashboards & Audit — User Guide](docs/DASHBOARDS_AND_AUDIT.md) — charts, dashboards, instance metrics, audit viewer
- [Drivers Overview](docs/DRIVERS.md) — supported databases, capabilities, limitations
- [Lua Scripting](docs/LUA.md) — the embedded Lua runtime for hooks

### Reference & internals

- [Architecture](ARCHITECTURE.md) — layered diagrams, query/connection flow, crate map
- [Charts](docs/CHARTS.md) — chart types, column kinds, axis auto-detection
- [Dashboards](docs/DASHBOARDS.md) — dashboards, saved charts, instance metrics and inspectors
- [Audit](docs/AUDIT.md) — audit event schema and redaction
- [AI + MCP Integration Guide](docs/MCP_AI_INTEGRATION.md)
- [Driver RPC Protocol](docs/DRIVER_RPC_PROTOCOL.md)
- [RPC Services Config](docs/RPC_SERVICES_CONFIG.md)
- [Release Process](docs/RELEASE.md)
- [Contributing](CONTRIBUTING.md)
- [Code Style](CODE_STYLE.md)
- [Agent Instructions](AGENTS.md)
- [Claude Instructions](CLAUDE.md)

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

Available in the AUR:

```bash
# Using an AUR helper
paru -S dbflux
# or
yay -S dbflux
```

#### Debian / Ubuntu

Download the `.deb` package from [Releases](https://github.com/0xErwin1/dbflux/releases):

```bash
# Replace amd64 with arm64 for ARM
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.deb
sudo dpkg -i dbflux-linux-amd64.deb
```

#### Fedora / RHEL / CentOS

Download the `.rpm` package from [Releases](https://github.com/0xErwin1/dbflux/releases):

```bash
# Replace amd64 with arm64 for ARM
sudo dnf install https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.rpm
```

#### Nix

Using flakes (the default package is a **prebuilt binary** for Linux x86_64 / aarch64, no compilation):

```bash
# Run directly (prebuilt)
nix run github:0xErwin1/dbflux

# Install to profile (prebuilt)
nix profile install github:0xErwin1/dbflux

# Development shell
nix develop github:0xErwin1/dbflux
```

Build from source instead of using the prebuilt binary:

```bash
nix run    github:0xErwin1/dbflux#dbflux-source
nix build  github:0xErwin1/dbflux#dbflux-source
```

Nightly builds track `main` and install side by side with stable (distinct app id, icon, and `dbflux-nightly.db` database). Consume them from the `nightly` ref:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
nix profile install github:0xErwin1/dbflux/nightly#dbflux-nightly
```

See [docs/RELEASE.md](docs/RELEASE.md) for the channel model.

NixOS / nix-darwin via overlay:

```nix
{
  inputs.dbflux.url = "github:0xErwin1/dbflux";

  outputs = { nixpkgs, dbflux, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        ({ pkgs, ... }: {
          nixpkgs.overlays = [ dbflux.overlays.default ];
          environment.systemPackages = [
            pkgs.dbflux         # prebuilt binary, no local compile
            # pkgs.dbflux-source  # alternative: build from source
          ];
        })
      ];
    };
  };
}
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

# Recommended: build with the full default feature set
cargo build --release --features sqlite,postgres,mysql,mssql,mongodb,redis,dynamodb,cloudwatch,influxdb,lua,aws,mcp

# Minimal build (relational drivers only, no AI/MCP, no Lua)
cargo build --release --no-default-features --features sqlite,postgres,mysql

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
## Features

### Database Support

- **PostgreSQL** with SSL/TLS modes (Disable, Prefer, Require)
- **MySQL** / MariaDB
- **SQLite** for local database files
- **Microsoft SQL Server** (TDS) with TLS, SQL Browser named-instance routing, and multi-schema introspection
- **MongoDB** with collection browsing, document CRUD, and shell query generation
- **Redis** with key browsing for all types (String, Hash, List, Set, Sorted Set, Stream)
- **DynamoDB** with table browsing, item CRUD, and AWS authentication
- **InfluxDB** v1 and v2 (InfluxQL on v1, InfluxQL + Flux on v2)
- **CloudWatch Logs** with log group/stream browsing and event streaming
- **External drivers over RPC** (register out-of-process drivers via the [Driver RPC Protocol](docs/DRIVER_RPC_PROTOCOL.md))

See [docs/DRIVERS.md](docs/DRIVERS.md) for a full capability matrix and per-driver limitations.

### User Interface

- Document-based workspace with multiple result tabs (like DBeaver/VS Code)
- Collapsible, resizable sidebar with ToggleSidebar command (Ctrl+B)
- Schema tree browser with lazy loading for large databases
- Schema-level metadata: indexes, foreign keys, constraints, custom types (PostgreSQL)
- Stored procedures / routines folder per schema (drivers that expose them)
- Multi-tab SQL editor with syntax highlighting and multi-statement execution (one result set per statement, where the driver supports it)
- Virtualized data table with column resizing, horizontal scrolling, and sorting
- Table browser with WHERE filters, custom LIMIT, and pagination
- Workspace inspector rail for row/document details
- "Copy as Query" context menu to copy INSERT/UPDATE/DELETE as SQL, MongoDB shell, or Redis commands
- Query preview modal with language-specific syntax highlighting
- Command palette with fuzzy search
- Custom toast notification system with auto-dismiss
- Background task panel
- Session restore: open tabs are restored on startup with conflict detection for externally modified files

### Visual Query Builder

- Right-rail SELECT builder: projection, joins, a nested WHERE predicate tree, ORDER BY, and LIMIT/OFFSET, with a live parameterized SQL preview
- GROUP BY with aggregates (COUNT, SUM, AVG, MIN, MAX) and HAVING
- Visual UPDATE / DELETE builder with mutation policies (read-only / approval-required) and chunked, cancellable execution
- Schema-aware autocomplete on builder inputs and the results WHERE filter
- Relational filters in the results filter bar via dotted foreign-key paths (e.g. `created_by.email LIKE '%@acme.com'`)
- Inline cell edit and row delete on builder-generated results when they map 1:1 to a single table
- Saved visual queries per connection
- SQL drivers only (SQLite, PostgreSQL, MySQL/MariaDB, SQL Server); driver-agnostic by construction

### Charts & Visualization

- Chart any query or collection result: Line, Bar, Scatter, Area, Stacked Bar, and Pie
- Automatic axis detection from column kinds (timestamp X axis, numeric Y series) — no per-driver heuristics
- Saved charts that reopen as their own document tab
- Dashboards: arrange saved charts, dividers, and inspector panels on a 12-column grid with a shared time range
- Read-only Instance Overview per connection — live server metrics and tabular inspectors, with "Save as editable"; PostgreSQL, MySQL/MariaDB, MongoDB, Redis, and SQL Server ship instance catalogs
- Browse and import upstream provider dashboards (CloudWatch)
- See [docs/CHARTS.md](docs/CHARTS.md) and [docs/DASHBOARDS.md](docs/DASHBOARDS.md) for details

### Connectivity & Access

- SSH tunnels with key, password, and agent authentication; reusable SSH tunnel profiles
- SOCKS5 / HTTP CONNECT proxy tunnels with reusable proxy profiles
- Managed access providers (AWS SSM) for connecting without exposing ports
- Provider-driven auth profiles (e.g. AWS SSO/shared/static), with import from `~/.aws/config`
- Connection hooks at PreConnect/PostConnect/PreDisconnect/PostDisconnect, runnable as a command, a script, or in-process Lua

### AI & MCP Integration

- Built-in Model Context Protocol (MCP) server (`dbflux mcp`) for AI clients
- Governance layer: operation classification, role/policy engine, trusted clients, and human approval flow for write/destructive operations
- See [docs/MCP_AI_INTEGRATION.md](docs/MCP_AI_INTEGRATION.md)

### Audit & Scripting

- SQLite-backed audit log for queries, connections, hooks, scripts, MCP, governance, and config events, with redaction and query fingerprinting — see [docs/AUDIT.md](docs/AUDIT.md)
- Centralized user-facing error reporting: failures surface as a toast with a correlation id and a "View in Audit" action, drive a status-bar error badge, and are correlated with their audit row
- Lua, Python, and Bash scripts run as documents with live streamed output — see [docs/LUA.md](docs/LUA.md)

### Keyboard Navigation

- Vim-style navigation (`j`/`k`/`h`/`l`) throughout the app
- Context-aware keybindings (Document, Sidebar, BackgroundTasks)
- Document focus with internal editor/results navigation
- Results toolbar: `f` to focus, `h`/`l` to navigate, `Enter` to edit/execute, `Esc` to exit
- Toggle sidebar with `Ctrl+B`
- Tab switching (MRU order) with `Ctrl+Tab` / `Ctrl+Shift+Tab`

### Query Management

- Query history with timestamps
- Saved queries with favorites
- Search across history and saved queries

### Export

- Shape-based export: CSV, JSON (pretty/compact), Text, Binary (raw/hex/base64)
- Export format determined by result type (table, JSON, text, binary)

## Development

### Prerequisites

On Linux, the `mold` linker is **required** for local builds: the repo's
`.cargo/config.toml` links the `x86_64-unknown-linux-gnu` target with
`-fuse-ld=mold` to cut link time and memory across the 60+ workspace crates.
The Nix dev shell provides it automatically; for non-Nix setups install it via
your package manager (included below). Windows and macOS use their default
linker and are unaffected.

**Ubuntu/Debian:**
```bash
sudo apt install pkg-config libssl-dev libdbus-1-dev libxkbcommon-dev mold
```

**Fedora:**
```bash
sudo dnf install pkg-config openssl-devel dbus-devel libxkbcommon-devel mold
```

**Arch:**
```bash
sudo pacman -S pkg-config openssl dbus libxkbcommon mold
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
cargo build -p dbflux --release
```

### Running

```bash
cargo run -p dbflux
```

### Commands

```bash
cargo check --workspace                    # Type checking
cargo clippy --workspace -- -D warnings    # Lint
cargo fmt --all                            # Format
cargo test --workspace                     # Tests
```

### Faster tests with nextest

[`cargo-nextest`](https://nexte.st) is the recommended test runner for this
workspace: it runs each test in its own process across a global pool, which is
noticeably faster than `cargo test` on a workspace this size. The Nix dev shell
provides it; otherwise install it from <https://nexte.st/docs/installation>.

```bash
cargo nextest run --workspace              # unit + integration tests
cargo test --doc --workspace               # doctests (nextest does not run these)
```

Live integration tests (normally `#[ignore]`d) use a different flag under nextest:

```bash
cargo nextest run -p dbflux_driver_sqlite --run-ignored all
```

### Nix Development Shell

If you use Nix, you can enter a development shell with all dependencies:

```bash
# With flakes
nix develop

# Traditional
nix-shell
```

## License

MIT & Apache-2.0
