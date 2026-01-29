#!/bin/bash
#
# DBFlux Linux Installer
#
# Usage:
#   Local (from repo):    ./install.sh [--prefix /usr/local]
#   Remote (curl):        curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash
#   Remote with options:  curl -fsSL <url> | bash -s -- --prefix ~/.local
#

set -euo pipefail

# Configuration
REPO_URL="https://github.com/0xErwin1/dbflux"
APP_NAME="dbflux"
DEFAULT_PREFIX="/usr/local"
GPG_KEY_ID="A614B7D25134987A"
GPG_KEYSERVER="keyserver.ubuntu.com"

# Detect script location and mode
if [[ -t 0 ]] && [[ -f "${BASH_SOURCE[0]:-}" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
    REMOTE_MODE=false
else
    # Running from curl pipe or stdin
    SCRIPT_DIR=""
    PROJECT_ROOT=""
    REMOTE_MODE=true
fi

# Color output (disable if not a terminal)
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

# Flags
DRY_RUN=false
PREFIX="${DEFAULT_PREFIX}"
BUILD_FROM_SOURCE=false
VERSION="latest"
SKIP_GPG_VERIFY=false

info() { echo -e "${GREEN}[INFO]${NC} $1" >&2; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
step() { echo -e "${BLUE}==>${NC} $1" >&2; }

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Install DBFlux on your Linux system.

OPTIONS:
    --prefix PATH       Installation prefix (default: $DEFAULT_PREFIX)
    --build             Build from source instead of downloading release
    --version VERSION   Install specific version (default: latest)
    --skip-gpg-verify   Skip GPG signature verification
    --dry-run           Show what would be done without making changes
    --help              Display this help message

INSTALLATION METHODS:
    # Install latest release (recommended)
    curl -fsSL $REPO_URL/raw/main/scripts/install.sh | bash

    # Install to user directory (no root required)
    curl -fsSL $REPO_URL/raw/main/scripts/install.sh | bash -s -- --prefix ~/.local

    # Build from source
    curl -fsSL $REPO_URL/raw/main/scripts/install.sh | bash -s -- --build

    # Local installation from cloned repo
    ./scripts/install.sh

PRIVILEGES:
    Root/sudo is only required if the prefix is not writable by the current user.
EOF
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --build)
            BUILD_FROM_SOURCE=true
            shift
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-gpg-verify)
            SKIP_GPG_VERIFY=true
            shift
            ;;
        --help|-h)
            usage
            ;;
        *)
            error "Unknown option: $1"
            usage
            ;;
    esac
done

# Detect architecture
detect_arch() {
    local arch
    arch=$(uname -m)

    case "$arch" in
        x86_64|amd64)
            echo "linux-amd64"
            ;;
        aarch64|arm64)
            echo "linux-arm64"
            ;;
        *)
            error "Unsupported architecture: $arch"
            exit 1
            ;;
    esac
}

# Detect OS
detect_os() {
    local os
    os=$(uname -s)

    case "$os" in
        Linux)
            echo "linux"
            ;;
        Darwin)
            error "macOS is not supported yet. Please build from source."
            exit 1
            ;;
        *)
            error "Unsupported operating system: $os"
            exit 1
            ;;
    esac
}

# Check required commands
check_requirements() {
    local missing=()

    if [[ "$REMOTE_MODE" == "true" ]] && [[ "$BUILD_FROM_SOURCE" == "false" ]]; then
        # For downloading releases
        if ! command -v curl &>/dev/null && ! command -v wget &>/dev/null; then
            missing+=("curl or wget")
        fi
        if ! command -v tar &>/dev/null; then
            missing+=("tar")
        fi
    fi

    if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
        if ! command -v cargo &>/dev/null; then
            missing+=("cargo (Rust toolchain)")
        fi
        if ! command -v git &>/dev/null; then
            missing+=("git")
        fi
        if ! command -v pkg-config &>/dev/null; then
            missing+=("pkg-config")
        fi
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing required commands: ${missing[*]}"
        echo ""
        echo "Install them with your package manager:"
        echo "  Ubuntu/Debian: sudo apt install ${missing[*]}"
        echo "  Fedora:        sudo dnf install ${missing[*]}"
        echo "  Arch:          sudo pacman -S ${missing[*]}"
        exit 1
    fi
}

# Check if prefix is writable
check_prefix_writable() {
    if [[ "$DRY_RUN" == "true" ]]; then
        return 0
    fi

    local test_dir="$PREFIX/.write_test_$$"

    if mkdir -p "$test_dir" 2>/dev/null; then
        rm -rf "$test_dir" 2>/dev/null || true
        return 0
    fi

    rm -rf "$test_dir" 2>/dev/null || true

    if [[ $EUID -ne 0 ]]; then
        error "Installation prefix '$PREFIX' is not writable"
        echo ""
        echo "Options:"
        echo "  1. Run with sudo: sudo $0 $*"
        echo "  2. Install to user directory: $0 --prefix ~/.local"
        exit 1
    fi
}

# Download file with curl or wget
download() {
    local url="$1"
    local output="$2"

    if command -v curl &>/dev/null; then
        curl -fsSL "$url" -o "$output"
    elif command -v wget &>/dev/null; then
        wget -q "$url" -O "$output"
    else
        error "Neither curl nor wget found"
        exit 1
    fi
}

# Get latest release version from GitHub
get_latest_version() {
    local api_url="https://api.github.com/repos/0xErwin1/dbflux/releases/latest"
    local version

    if command -v curl &>/dev/null; then
        version=$(curl -fsSL "$api_url" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
    elif command -v wget &>/dev/null; then
        version=$(wget -qO- "$api_url" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
    fi

    if [[ -z "$version" ]]; then
        error "Failed to get latest version from GitHub"
        exit 1
    fi

    echo "$version"
}

# Import GPG key from keyserver
import_gpg_key() {
    if gpg --list-keys "$GPG_KEY_ID" &>/dev/null; then
        return 0
    fi

    step "Importing GPG key $GPG_KEY_ID from $GPG_KEYSERVER..."
    if ! gpg --keyserver "$GPG_KEYSERVER" --recv-keys "$GPG_KEY_ID" &>/dev/null; then
        warn "Failed to import GPG key from keyserver"
        return 1
    fi
    info "GPG key imported"
}

# Verify GPG signature
verify_gpg_signature() {
    local file="$1"
    local sig_file="$2"

    if [[ "$SKIP_GPG_VERIFY" == "true" ]]; then
        warn "Skipping GPG verification (--skip-gpg-verify)"
        return 0
    fi

    if ! command -v gpg &>/dev/null; then
        warn "GPG not installed, skipping signature verification"
        warn "Install gnupg and re-run, or use --skip-gpg-verify"
        return 0
    fi

    import_gpg_key || {
        warn "Could not import GPG key, skipping signature verification"
        return 0
    }

    step "Verifying GPG signature..."
    if gpg --verify "$sig_file" "$file" &>/dev/null; then
        info "GPG signature OK"
        return 0
    else
        error "GPG signature verification failed!"
        error "The file may have been tampered with."
        echo ""
        echo "To skip verification (not recommended): $0 --skip-gpg-verify"
        exit 1
    fi
}

# Download and extract release
download_release() {
    local arch="$1"
    local version="$2"
    local tmp_dir="$3"

    local release_url="$REPO_URL/releases/download/$version/dbflux-$arch.tar.gz"
    local checksum_url="$REPO_URL/releases/download/$version/dbflux-$arch.tar.gz.sha256"
    local sig_url="$REPO_URL/releases/download/$version/dbflux-$arch.tar.gz.asc"

    step "Downloading DBFlux $version for $arch..."

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] Would download: $release_url"
        return 0
    fi

    local tarball="$tmp_dir/dbflux.tar.gz"
    local checksum_file="$tmp_dir/dbflux.tar.gz.sha256"
    local sig_file="$tmp_dir/dbflux.tar.gz.asc"

    download "$release_url" "$tarball"
    download "$checksum_url" "$checksum_file"
    download "$sig_url" "$sig_file" || true  # Signature might not exist for older releases

    # Verify GPG signature first (if available)
    if [[ -f "$sig_file" ]]; then
        verify_gpg_signature "$tarball" "$sig_file"
    else
        warn "No GPG signature found for this release"
    fi

    # Verify checksum
    step "Verifying checksum..."
    cd "$tmp_dir"
    if ! sha256sum -c "$checksum_file" &>/dev/null; then
        error "Checksum verification failed!"
        exit 1
    fi
    info "Checksum OK"

    step "Extracting..."
    tar -xzf "$tarball" -C "$tmp_dir"
}

# Clone and build from source
build_from_source() {
    local tmp_dir="$1"
    local version="$2"

    step "Building DBFlux from source..."

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] Would clone and build from source"
        return 0
    fi

    local repo_dir="$tmp_dir/dbflux"

    # Clone repository
    if [[ -n "$PROJECT_ROOT" ]] && [[ -f "$PROJECT_ROOT/Cargo.toml" ]]; then
        info "Using local repository at $PROJECT_ROOT"
        repo_dir="$PROJECT_ROOT"
    else
        step "Cloning repository..."
        git clone --depth 1 "$REPO_URL.git" "$repo_dir"

        if [[ "$version" != "latest" ]]; then
            cd "$repo_dir"
            git fetch --depth 1 origin tag "$version"
            git checkout "$version"
        fi
    fi

    cd "$repo_dir"

    # Check for build dependencies
    step "Checking build dependencies..."
    check_build_deps

    # Build
    step "Compiling (this may take a few minutes)..."
    cargo build --release --features sqlite,postgres,mysql

    # Create package structure in tmp_dir
    mkdir -p "$tmp_dir/pkg/resources/icons"
    mkdir -p "$tmp_dir/pkg/resources/desktop"
    mkdir -p "$tmp_dir/pkg/resources/mime"
    mkdir -p "$tmp_dir/pkg/scripts"

    cp "$repo_dir/target/release/dbflux" "$tmp_dir/pkg/dbflux"
    chmod +x "$tmp_dir/pkg/dbflux"

    if [[ -f "$repo_dir/resources/icons/dbflux.svg" ]]; then
        cp "$repo_dir/resources/icons/dbflux.svg" "$tmp_dir/pkg/resources/icons/"
    fi
    if [[ -f "$repo_dir/resources/desktop/dbflux.desktop" ]]; then
        cp "$repo_dir/resources/desktop/dbflux.desktop" "$tmp_dir/pkg/resources/desktop/"
    fi
    if [[ -f "$repo_dir/resources/mime/dbflux-sql.xml" ]]; then
        cp "$repo_dir/resources/mime/dbflux-sql.xml" "$tmp_dir/pkg/resources/mime/"
    fi
}

# Check build dependencies
check_build_deps() {
    local missing=()

    # Check for required dev libraries
    if ! pkg-config --exists openssl 2>/dev/null; then
        missing+=("libssl-dev")
    fi
    if ! pkg-config --exists dbus-1 2>/dev/null; then
        missing+=("libdbus-1-dev")
    fi
    if ! pkg-config --exists xkbcommon 2>/dev/null; then
        missing+=("libxkbcommon-dev")
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        warn "Missing build dependencies: ${missing[*]}"
        echo ""
        echo "Install them with:"
        echo "  Ubuntu/Debian: sudo apt install ${missing[*]}"
        echo "  Fedora:        sudo dnf install ${missing[*]//lib/} ${missing[*]//-dev/-devel}"
        echo "  Arch:          sudo pacman -S ${missing[*]//lib/} ${missing[*]//-dev/}"
        echo ""
        read -p "Continue anyway? [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
}

# Install files
install_files() {
    local src_dir="$1"

    step "Installing to $PREFIX..."

    # Binary
    if [[ -f "$src_dir/dbflux" ]]; then
        mkdir_safe "$PREFIX/bin"
        cp_safe "$src_dir/dbflux" "$PREFIX/bin/dbflux"
        chmod_safe "$PREFIX/bin/dbflux" 755
    fi

    # Desktop entry
    if [[ -f "$src_dir/resources/desktop/dbflux.desktop" ]]; then
        mkdir_safe "$PREFIX/share/applications"
        cp_safe "$src_dir/resources/desktop/dbflux.desktop" "$PREFIX/share/applications/dbflux.desktop"
        chmod_safe "$PREFIX/share/applications/dbflux.desktop" 644
    fi

    # Icon
    if [[ -f "$src_dir/resources/icons/dbflux.svg" ]]; then
        mkdir_safe "$PREFIX/share/icons/hicolor/scalable/apps"
        cp_safe "$src_dir/resources/icons/dbflux.svg" "$PREFIX/share/icons/hicolor/scalable/apps/dbflux.svg"
        chmod_safe "$PREFIX/share/icons/hicolor/scalable/apps/dbflux.svg" 644
    fi

    # MIME type
    if [[ -f "$src_dir/resources/mime/dbflux-sql.xml" ]]; then
        mkdir_safe "$PREFIX/share/mime/packages"
        cp_safe "$src_dir/resources/mime/dbflux-sql.xml" "$PREFIX/share/mime/packages/dbflux-sql.xml"
        chmod_safe "$PREFIX/share/mime/packages/dbflux-sql.xml" 644

        if [[ "$DRY_RUN" == "false" ]] && command -v update-mime-database &>/dev/null; then
            update-mime-database "$PREFIX/share/mime" 2>/dev/null || true
        fi
    fi
}

mkdir_safe() {
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] mkdir -p $1"
    else
        mkdir -p "$1"
    fi
}

cp_safe() {
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] cp $1 $2"
    else
        cp "$1" "$2"
    fi
}

chmod_safe() {
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] chmod $2 $1"
    else
        chmod "$2" "$1"
    fi
}

# Check for existing installation
check_existing() {
    if [[ -x "$PREFIX/bin/$APP_NAME" ]]; then
        warn "DBFlux is already installed at $PREFIX/bin/$APP_NAME"

        if [[ -t 0 ]]; then
            read -p "Overwrite existing installation? [y/N] " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                info "Installation cancelled"
                exit 0
            fi
        else
            info "Overwriting existing installation (non-interactive mode)"
        fi
    fi
}

# Post-install message
post_install() {
    echo ""
    info "DBFlux installed successfully!"
    echo ""

    local bin_dir="$PREFIX/bin"
    if [[ ":$PATH:" != *":$bin_dir:"* ]]; then
        warn "Installation directory is not in PATH: $bin_dir"
        echo ""
        echo "Add it to your PATH:"
        echo "  echo 'export PATH=\"$bin_dir:\$PATH\"' >> ~/.bashrc"
        echo "  source ~/.bashrc"
        echo ""
    fi

    echo "Run 'dbflux' to start the application."
    echo ""
    echo "To uninstall:"
    if [[ "$REMOTE_MODE" == "true" ]]; then
        echo "  curl -fsSL $REPO_URL/raw/main/scripts/uninstall.sh | bash -s -- --prefix $PREFIX"
    else
        echo "  $SCRIPT_DIR/uninstall.sh --prefix $PREFIX"
    fi
}

# Main
main() {
    echo ""
    echo "  ╔══════════════════════════════════════╗"
    echo "  ║       DBFlux Linux Installer         ║"
    echo "  ╚══════════════════════════════════════╝"
    echo ""

    detect_os >/dev/null
    local arch
    arch=$(detect_arch)
    info "Detected architecture: $arch"

    check_requirements
    check_prefix_writable

    # Resolve version
    if [[ "$VERSION" == "latest" ]] && [[ "$BUILD_FROM_SOURCE" == "false" ]]; then
        step "Fetching latest version..."
        VERSION=$(get_latest_version)
    fi
    info "Version: $VERSION"

    check_existing

    # Create temp directory
    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap "rm -rf '$tmp_dir'" EXIT

    # Download or build
    if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
        build_from_source "$tmp_dir" "$VERSION"
        install_files "$tmp_dir/pkg"
    elif [[ "$REMOTE_MODE" == "false" ]] && [[ -x "$PROJECT_ROOT/dbflux" ]]; then
        # Local mode from extracted release tarball
        info "Using binary from extracted release package"
        install_files "$PROJECT_ROOT"
    elif [[ "$REMOTE_MODE" == "false" ]] && [[ -f "$PROJECT_ROOT/target/release/dbflux" ]]; then
        # Local mode with compiled binary (dev environment)
        info "Using existing binary from $PROJECT_ROOT/target/release/"
        mkdir -p "$tmp_dir/pkg"
        cp "$PROJECT_ROOT/target/release/dbflux" "$tmp_dir/pkg/"
        cp -r "$PROJECT_ROOT/resources" "$tmp_dir/pkg/" 2>/dev/null || true
        install_files "$tmp_dir/pkg"
    elif [[ "$REMOTE_MODE" == "false" ]] && [[ -f "$PROJECT_ROOT/Cargo.toml" ]]; then
        # Local mode without binary - build it
        info "Binary not found, building from source..."
        BUILD_FROM_SOURCE=true
        build_from_source "$tmp_dir" "$VERSION"
        install_files "$tmp_dir/pkg"
    else
        # Remote mode - download release
        download_release "$arch" "$VERSION" "$tmp_dir"
        install_files "$tmp_dir"
    fi

    if [[ "$DRY_RUN" == "false" ]]; then
        post_install
    else
        echo ""
        info "Dry-run completed. No changes were made."
    fi
}

main "$@"
