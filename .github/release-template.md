## Installation

### Linux

### Tarball (recommended)
```bash
# Download and extract (replace amd64 with arm64 for ARM)
wget https://github.com/__REPO__/releases/download/__VERSION__/dbflux-linux-amd64.tar.gz
tar -xzf dbflux-linux-amd64.tar.gz

# Run installer
sudo ./scripts/install.sh
```

### AppImage (portable)
```bash
# Download
wget https://github.com/__REPO__/releases/download/__VERSION__/dbflux-linux-amd64.AppImage
chmod +x dbflux-linux-amd64.AppImage
./dbflux-linux-amd64.AppImage
```

### macOS

DBFlux for macOS is not signed with an Apple developer certificate. When opening for the first time:

1. Download the DMG for your architecture:
   - **Intel Macs**: `dbflux-macos-amd64.dmg`
   - **Apple Silicon (M1/M2/M3)**: `dbflux-macos-arm64.dmg`
2. Open the DMG and drag DBFlux to Applications
3. When you see "unidentified developer", go to **System Preferences → Privacy & Security**
4. Click **Open Anyway** next to the warning
5. Confirm you want to open the application

Alternatively, from Terminal:
```bash
xattr -cr /Applications/DBFlux.app
```

### Windows

#### Installer
1. Download `dbflux-windows-amd64-setup.exe`
2. Run the installer and follow the wizard

#### Portable
1. Download `dbflux-windows-amd64.zip`
2. Extract and run `dbflux.exe`

> Note: The executable is not signed. Windows SmartScreen may show a warning. Click "More info" → "Run anyway".

---

## Verify Downloads

Releases triggered with `workflow_dispatch` and `sign=true` include GPG signatures (key `A614B7D25134987A`).

```bash
# Import the public key (one time)
gpg --keyserver keyserver.ubuntu.com --recv-keys A614B7D25134987A

# Verify checksum
sha256sum -c dbflux-linux-amd64.tar.gz.sha256

# Verify GPG signature (if signed)
gpg --verify dbflux-linux-amd64.tar.gz.asc dbflux-linux-amd64.tar.gz
```

## System Requirements

| Platform | Requirements |
|----------|-------------|
| Linux | x86_64 or ARM64, Vulkan-capable GPU (recommended) |
| macOS | macOS 11.0 (Big Sur) or later |
| Windows | Windows 10 or later, x86_64 |

---
