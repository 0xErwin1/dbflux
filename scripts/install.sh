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
INSTALL_SCRIPT_URL="https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh"
UNINSTALL_SCRIPT_URL="https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh"
APP_NAME="dbflux"
DEFAULT_PREFIX="/usr/local"

# Full fingerprint of the release signing key. Signatures are accepted only
# when their primary key matches it, so a short-ID collision on the keyserver
# cannot substitute another key. Update it when the signing key is rotated.
GPG_KEY_FINGERPRINT="B39EB98E8860DAFB05670073A614B7D25134987A"
GPG_KEY_URL="https://keyserver.ubuntu.com/pks/lookup?op=get&options=mr&search=0x${GPG_KEY_FINGERPRINT}"

ORIGINAL_ARGS=("$@")

# A script read from a pipe (`curl | bash`, `bash -s`) or a process
# substitution has no regular file behind BASH_SOURCE. Whether stdin is a
# terminal is irrelevant: a local run under automation is still a local run.
if [[ -f "${BASH_SOURCE[0]:-}" ]]; then
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
    exit "${1:-1}"
}

# Fail with a clear message when an option that takes a value is the last argument.
require_value() {
    local option="$1"
    local remaining="$2"

    if [[ "$remaining" -lt 2 ]]; then
        error "Option $option requires a value"
        usage 1
    fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --prefix)
            require_value "$1" "$#"
            PREFIX="$2"
            shift 2
            ;;
        --build)
            BUILD_FROM_SOURCE=true
            shift
            ;;
        --version)
            require_value "$1" "$#"
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
            usage 0
            ;;
        *)
            error "Unknown option: $1"
            usage 1
            ;;
    esac
done

if [[ "$PREFIX" != /* ]]; then
    error "Installation prefix must be an absolute path: $PREFIX"
    exit 1
fi

# Release tags are `vX.Y.Z`; accept `X.Y.Z` as well instead of failing with a 404.
if [[ "$VERSION" =~ ^[0-9]+\.[0-9]+ ]]; then
    VERSION="v$VERSION"
fi

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
        if ! command -v sha256sum &>/dev/null; then
            missing+=("sha256sum (coreutils)")
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
        echo "" >&2
        echo "Options:" >&2
        echo "  1. Run with sudo: $(rerun_command sudo "${ORIGINAL_ARGS[@]}")" >&2
        echo "  2. Install to user directory: $(rerun_command "" --prefix "$HOME/.local")" >&2
        exit 1
    fi
}

# Print the command that re-runs this installer the way it was started
# (piped from curl or as a local file), optionally under sudo.
rerun_command() {
    local runner="$1"
    shift

    local arguments=""
    if [[ $# -gt 0 ]]; then
        arguments=" $(printf '%q ' "$@")"
        arguments="${arguments% }"
    fi

    local sudo_prefix=""
    if [[ -n "$runner" ]]; then
        sudo_prefix="$runner "
    fi

    if [[ "$REMOTE_MODE" == "true" ]]; then
        if [[ -n "$arguments" ]]; then
            echo "curl -fsSL $INSTALL_SCRIPT_URL | ${sudo_prefix}bash -s --$arguments"
        else
            echo "curl -fsSL $INSTALL_SCRIPT_URL | ${sudo_prefix}bash"
        fi
    else
        echo "${sudo_prefix}$0$arguments"
    fi
}

# Download a URL with curl or wget; an output of "-" writes to stdout.
# Returns the downloader's status instead of exiting, so callers decide
# whether a missing file is fatal.
download() {
    local url="$1"
    local output="$2"

    if command -v curl &>/dev/null; then
        curl -fsSL --proto '=https' --retry 3 "$url" -o "$output"
    elif command -v wget &>/dev/null; then
        wget -q "$url" -O "$output"
    else
        error "Neither curl nor wget found"
        exit 1
    fi
}

# Download a file the installation cannot proceed without.
download_required() {
    local url="$1"
    local output="$2"

    if ! download "$url" "$output"; then
        error "Failed to download $url"
        error "Check the version and architecture, or your network connection."
        exit 1
    fi
}

# Get latest release version from GitHub
get_latest_version() {
    local api_url="https://api.github.com/repos/0xErwin1/dbflux/releases/latest"
    local response

    if ! response=$(download "$api_url" -); then
        error "Failed to query the latest release from $api_url"
        error "The GitHub API may be rate limiting this address; retry later or pass --version vX.Y.Z."
        exit 1
    fi

    local version
    version=$(printf '%s\n' "$response" | grep -m1 '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/' || true)

    if [[ ! "$version" =~ ^v[0-9] ]]; then
        error "Failed to read the latest version from the GitHub API response"
        exit 1
    fi

    echo "$version"
}

# Verify a detached signature against the pinned release key.
#
# Uses a throwaway GnuPG home inside the installer's temporary directory, so
# the user's keyring is neither read nor modified and only the pinned key can
# produce an accepted signature. The key is fetched over HTTPS, which works
# without dirmngr and through firewalls that block the HKP port.
#
# Missing gpg or an unreachable key server degrade to a warning (the checksum
# still guards against corruption); a signature that does not verify aborts.
verify_gpg_signature() {
    local file="$1"
    local sig_file="$2"
    local gnupg_home="$3"

    if [[ "$SKIP_GPG_VERIFY" == "true" ]]; then
        warn "Skipping GPG verification (--skip-gpg-verify)"
        return 0
    fi

    if ! command -v gpg &>/dev/null; then
        warn "GPG not installed, skipping signature verification"
        warn "Install gnupg and re-run, or use --skip-gpg-verify"
        return 0
    fi

    mkdir -p "$gnupg_home"
    chmod 700 "$gnupg_home"

    step "Importing release signing key $GPG_KEY_FINGERPRINT..."
    local key_file="$gnupg_home/release-key.asc"
    if ! download "$GPG_KEY_URL" "$key_file" \
        || ! gpg --homedir "$gnupg_home" --batch --quiet --import "$key_file" &>/dev/null; then
        warn "Could not import the release signing key, skipping signature verification"
        return 0
    fi

    step "Verifying GPG signature..."
    local status_output
    status_output=$(gpg --homedir "$gnupg_home" --batch --status-fd 1 --verify "$sig_file" "$file" 2>/dev/null || true)

    # VALIDSIG carries the signing key's fingerprint and, as its last field
    # (when present), the primary key's fingerprint.
    local signer_fingerprints
    signer_fingerprints=$(printf '%s\n' "$status_output" \
        | awk '$1 == "[GNUPG:]" && $2 == "VALIDSIG" { print $3; print $NF }')

    if grep -qx "$GPG_KEY_FINGERPRINT" <<< "$signer_fingerprints"; then
        info "GPG signature OK"
        return 0
    fi

    error "GPG signature verification failed!"
    error "The file may have been tampered with."
    echo "" >&2
    echo "To skip verification (not recommended): $(rerun_command "" "${ORIGINAL_ARGS[@]}" --skip-gpg-verify)" >&2
    exit 1
}

# Compare a file's SHA-256 against a `sha256sum`-format checksum file.
#
# The expected hash is read from the first field and compared directly, so the
# file name recorded in the checksum file does not have to match the local one.
verify_checksum() {
    local file="$1"
    local checksum_file="$2"

    step "Verifying checksum..."

    local expected_checksum
    local actual_checksum
    expected_checksum=$(awk 'NR == 1 { print tolower($1) }' "$checksum_file")
    actual_checksum=$(sha256sum "$file" | awk '{ print $1 }')

    if [[ ! "$expected_checksum" =~ ^[0-9a-f]{64}$ ]]; then
        error "Checksum file is empty or malformed"
        exit 1
    fi

    if [[ "$expected_checksum" != "$actual_checksum" ]]; then
        error "Checksum verification failed!"
        error "Expected: $expected_checksum"
        error "Actual:   $actual_checksum"
        exit 1
    fi

    info "Checksum OK"
}

# Download and extract release
download_release() {
    local arch="$1"
    local version="$2"
    local tmp_dir="$3"

    local asset_name="dbflux-$arch.tar.gz"
    local release_url="$REPO_URL/releases/download/$version/$asset_name"
    local checksum_url="$release_url.sha256"
    local sig_url="$release_url.asc"

    step "Downloading DBFlux $version for $arch..."

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] Would download: $release_url"
        return 0
    fi

    local tarball="$tmp_dir/$asset_name"
    local checksum_file="$tarball.sha256"
    local sig_file="$tarball.asc"

    download_required "$release_url" "$tarball"
    download_required "$checksum_url" "$checksum_file"

    # wget leaves an empty file behind on a failed download, so a missing
    # signature is detected by the download status rather than by the file.
    if download "$sig_url" "$sig_file"; then
        verify_gpg_signature "$tarball" "$sig_file" "$tmp_dir/gnupg"
    else
        rm -f "$sig_file"
        warn "No GPG signature found for this release"
    fi

    verify_checksum "$tarball" "$checksum_file"

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
    mkdir -p "$tmp_dir/pkg/resources/branding/stable"
    mkdir -p "$tmp_dir/pkg/resources/desktop"
    mkdir -p "$tmp_dir/pkg/resources/mime"
    mkdir -p "$tmp_dir/pkg/scripts"

    cp "$repo_dir/target/release/dbflux" "$tmp_dir/pkg/dbflux"
    chmod +x "$tmp_dir/pkg/dbflux"

    if [[ -f "$repo_dir/resources/branding/stable/mark.svg" ]]; then
        cp "$repo_dir/resources/branding/stable/mark.svg" "$tmp_dir/pkg/resources/branding/stable/mark.svg"
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

    # .cargo/config.toml links the x86_64 Linux target with mold.
    if [[ "$(uname -m)" == "x86_64" ]] && ! command -v mold &>/dev/null; then
        missing+=("mold")
    fi

    if ! pkg-config --exists openssl 2>/dev/null; then
        missing+=("libssl-dev")
    fi
    if ! pkg-config --exists dbus-1 2>/dev/null; then
        missing+=("libdbus-1-dev")
    fi
    if ! pkg-config --exists xkbcommon 2>/dev/null; then
        missing+=("libxkbcommon-dev")
    fi
    if ! pkg-config --exists xkbcommon-x11 2>/dev/null; then
        missing+=("libxkbcommon-x11-dev")
    fi

    if [[ ${#missing[@]} -eq 0 ]]; then
        return 0
    fi

    warn "Missing build dependencies: ${missing[*]}"
    echo "" >&2
    echo "Install the build dependencies with:" >&2
    echo "  Ubuntu/Debian: sudo apt install ${missing[*]}" >&2
    echo "  Fedora:        sudo dnf install mold openssl-devel dbus-devel libxkbcommon-devel libxkbcommon-x11-devel" >&2
    echo "  Arch:          sudo pacman -S mold openssl dbus libxkbcommon libxkbcommon-x11" >&2
    echo "" >&2

    # When piped from curl, stdin is the script itself, so the answer must
    # come from the terminal; without one, stop rather than guess.
    local reply=""
    if ! { read -p "Continue anyway? [y/N] " -n 1 -r reply < /dev/tty; } 2>/dev/null; then
        error "No terminal available to confirm; install the dependencies and re-run"
        exit 1
    fi
    echo >&2

    if [[ ! "$reply" =~ ^[Yy]$ ]]; then
        exit 1
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
        if [[ "$DRY_RUN" == "true" ]]; then
            echo "[DRY-RUN] cp $src_dir/resources/desktop/dbflux.desktop $PREFIX/share/applications/dbflux.desktop"
            echo "[DRY-RUN] sed -i 's|@EXEC_PATH@|$PREFIX/bin/dbflux|g' $PREFIX/share/applications/dbflux.desktop"
        else
            cp "$src_dir/resources/desktop/dbflux.desktop" "$PREFIX/share/applications/dbflux.desktop"
            # Resolve the binary path and the stable branding placeholders. This
            # installer ships the stable identity; per-channel coexistence is
            # provided by the deb/rpm/AppImage artifacts.
            sed -i \
                -e "s|@EXEC_PATH@|$PREFIX/bin/dbflux|g" \
                -e "s|@APP_NAME@|DBFlux|g" \
                -e "s|@APP_ID@|dbflux|g" \
                "$PREFIX/share/applications/dbflux.desktop"
            chmod 644 "$PREFIX/share/applications/dbflux.desktop"
        fi
    fi

    # Icon
    if [[ -f "$src_dir/resources/branding/stable/mark.svg" ]]; then
        mkdir_safe "$PREFIX/share/icons/hicolor/scalable/apps"
        cp_safe "$src_dir/resources/branding/stable/mark.svg" "$PREFIX/share/icons/hicolor/scalable/apps/dbflux.svg"
        chmod_safe "$PREFIX/share/icons/hicolor/scalable/apps/dbflux.svg" 644
    fi

    # MIME type
    if [[ -f "$src_dir/resources/mime/dbflux-sql.xml" ]]; then
        mkdir_safe "$PREFIX/share/mime/packages"
        cp_safe "$src_dir/resources/mime/dbflux-sql.xml" "$PREFIX/share/mime/packages/dbflux-sql.xml"
        if [[ "$DRY_RUN" == "false" ]]; then
            sed -i "s|@APP_ID@|dbflux|g" "$PREFIX/share/mime/packages/dbflux-sql.xml"
        fi
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
        local sudo_prefix=""
        if [[ $EUID -eq 0 ]]; then
            sudo_prefix="sudo "
        fi
        echo "  curl -fsSL $UNINSTALL_SCRIPT_URL | ${sudo_prefix}bash -s -- --prefix $PREFIX"
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
    # Expanded now on purpose: tmp_dir is local and gone when the EXIT trap runs.
    # shellcheck disable=SC2064
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
