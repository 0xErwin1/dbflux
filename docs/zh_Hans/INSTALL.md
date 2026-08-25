# Installing DBFlux

## Linux

### Tarball (recommended)

```bash
# Install to /usr/local (requires sudo)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | sudo bash

# Install to ~/.local (no sudo required)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --prefix ~/.local
```

### AppImage (portable)

```bash
# Download from releases (replace amd64 with arm64 for ARM)
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.AppImage
chmod +x dbflux-linux-amd64.AppImage
./dbflux-linux-amd64.AppImage
```

### Arch Linux

Available in the AUR:

```bash
# Using an AUR helper
paru -S dbflux
# or
yay -S dbflux
```

### Debian / Ubuntu

Download the `.deb` package from [Releases](https://github.com/0xErwin1/dbflux/releases):

```bash
# Replace amd64 with arm64 for ARM
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.deb
sudo dpkg -i dbflux-linux-amd64.deb
```

### Fedora / RHEL / CentOS

Download the `.rpm` package from [Releases](https://github.com/0xErwin1/dbflux/releases):

```bash
# Replace amd64 with arm64 for ARM
sudo dnf install https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.rpm
```

### Nix

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

See [docs/RELEASE.md](RELEASE.md) for the channel model.

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

## macOS

DBFlux for macOS is not signed with an Apple developer certificate. When opening for the first time, you'll see a warning about an "unidentified developer".

### Installation

1. Download the DMG for your architecture from [Releases](https://github.com/0xErwin1/dbflux/releases):
   - **Intel Macs**: `dbflux-macos-amd64.dmg`
   - **Apple Silicon (M1/M2/M3/M4)**: `dbflux-macos-arm64.dmg`
2. Open the DMG and drag DBFlux to Applications
3. When you see the "unidentified developer" warning:
   - Go to **System Settings → Privacy & Security**
   - Click **Open Anyway** next to the security warning
   - Confirm you want to open the application

### Bypass Gatekeeper from Terminal

```bash
# Remove quarantine attribute (allows opening without GUI confirmation)
xattr -cr /Applications/DBFlux.app

# Now you can open it normally
open /Applications/DBFlux.app
```

### Requirements

- macOS 11.0 (Big Sur) or later

## Windows

### Installer

1. Download `dbflux-windows-amd64-setup.exe` from [Releases](https://github.com/0xErwin1/dbflux/releases)
2. Run the installer and follow the wizard

### Portable

1. Download `dbflux-windows-amd64.zip` from [Releases](https://github.com/0xErwin1/dbflux/releases)
2. Extract to any folder
3. Run `dbflux.exe`

> **Note**: The executable is not signed with a Windows code signing certificate. Windows SmartScreen may show a warning. Click "More info" → "Run anyway" to proceed.

### Requirements

- Windows 10 or later
- x86_64 (ARM64 not yet supported)

## Build from Source

```bash
# Via install script (Linux)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --build

# Or manually
git clone https://github.com/0xErwin1/dbflux.git
cd dbflux

# Recommended: build with the full default feature set
cargo build --release --features sqlite,postgres,mysql,mssql,mongodb,redis,dynamodb,cloudwatch,influxdb,clickhouse,lua,aws,mcp

# Minimal build (relational drivers only, no AI/MCP, no Lua)
cargo build --release --no-default-features --features sqlite,postgres,mysql

./target/release/dbflux
```

## Uninstall (Linux)

```bash
# If installed with install.sh
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | sudo bash

# From ~/.local
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | bash -s -- --prefix ~/.local

# Remove user config and data too
./scripts/uninstall.sh --remove-config
```

## Next steps

- [Usage Guide](USAGE.md) — first launch, creating a connection, and running your first query
- [Connecting — Advanced Setup](CONNECTIONS.md) — SSH tunnels, proxies, AWS SSO and value sources
