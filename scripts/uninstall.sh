#!/bin/bash
#
# DBFlux Linux Uninstaller
#
# Usage:
#   Local:   ./uninstall.sh [--prefix /usr/local]
#   Remote:  curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | bash
#

set -euo pipefail

# Configuration
REPO_URL="https://github.com/0xErwin1/dbflux"
APP_NAME="dbflux"
DEFAULT_PREFIX="/usr/local"

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
REMOVE_CONFIG=false
FORCE=false

info() { echo -e "${GREEN}[INFO]${NC} $1" >&2; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
step() { echo -e "${BLUE}==>${NC} $1" >&2; }

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Uninstall DBFlux from your Linux system.

OPTIONS:
    --prefix PATH       Installation prefix (default: $DEFAULT_PREFIX)
    --remove-config     Also remove user configuration and data
    --force             Skip confirmation prompt
    --dry-run           Show what would be done without making changes
    --help              Display this help message

EXAMPLES:
    # Uninstall from default location
    $0

    # Uninstall from user directory
    $0 --prefix ~/.local

    # Uninstall and remove all data
    $0 --remove-config

    # Remote uninstall
    curl -fsSL $REPO_URL/raw/main/scripts/uninstall.sh | bash -s -- --prefix ~/.local

PRIVILEGES:
    Root/sudo is only required if prefix is not writable by current user.
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
        --remove-config)
            REMOVE_CONFIG=true
            shift
            ;;
        --force|-f)
            FORCE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
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
        echo "  2. Specify correct prefix: $0 --prefix ~/.local"
        exit 1
    fi
}

# Find installed files
find_installed_files() {
    local files=()

    [[ -f "$PREFIX/bin/$APP_NAME" ]] && files+=("$PREFIX/bin/$APP_NAME")
    [[ -f "$PREFIX/share/applications/$APP_NAME.desktop" ]] && files+=("$PREFIX/share/applications/$APP_NAME.desktop")
    [[ -f "$PREFIX/share/icons/hicolor/scalable/apps/$APP_NAME.svg" ]] && files+=("$PREFIX/share/icons/hicolor/scalable/apps/$APP_NAME.svg")
    [[ -f "$PREFIX/share/mime/packages/$APP_NAME-sql.xml" ]] && files+=("$PREFIX/share/mime/packages/$APP_NAME-sql.xml")

    # Check other icon sizes
    for size in 48x48 64x64 128x128 256x256; do
        [[ -f "$PREFIX/share/icons/hicolor/$size/apps/$APP_NAME.svg" ]] && files+=("$PREFIX/share/icons/hicolor/$size/apps/$APP_NAME.svg")
        [[ -f "$PREFIX/share/icons/hicolor/$size/apps/$APP_NAME.png" ]] && files+=("$PREFIX/share/icons/hicolor/$size/apps/$APP_NAME.png")
    done

    echo "${files[@]}"
}

# Find user config directories
find_user_config() {
    local dirs=()

    local config_dir="${XDG_CONFIG_HOME:-$HOME/.config}"
    local data_dir="${XDG_DATA_HOME:-$HOME/.local/share}"

    [[ -d "$config_dir/$APP_NAME" ]] && dirs+=("$config_dir/$APP_NAME")
    [[ -d "$data_dir/$APP_NAME" ]] && dirs+=("$data_dir/$APP_NAME")

    echo "${dirs[@]}"
}

# Remove file safely
rm_safe() {
    local file="$1"

    if [[ ! -e "$file" ]]; then
        return
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] rm $file"
    else
        rm -f "$file"
        info "Removed: $file"
    fi
}

# Remove directory safely (only if empty)
rmdir_safe() {
    local dir="$1"

    if [[ ! -d "$dir" ]]; then
        return
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] rmdir $dir (if empty)"
    else
        rmdir "$dir" 2>/dev/null || true
    fi
}

# Remove directory recursively
rm_rf_safe() {
    local dir="$1"

    if [[ ! -d "$dir" ]]; then
        return
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] rm -rf $dir"
    else
        rm -rf "$dir"
        info "Removed: $dir"
    fi
}

# Remove binary
remove_binary() {
    step "Removing binary..."
    rm_safe "$PREFIX/bin/$APP_NAME"
}

# Remove desktop entry
remove_desktop_entry() {
    step "Removing desktop entry..."
    rm_safe "$PREFIX/share/applications/$APP_NAME.desktop"
    rmdir_safe "$PREFIX/share/applications"
}

# Remove icons
remove_icons() {
    step "Removing icons..."

    local icon_dir="$PREFIX/share/icons/hicolor"
    local sizes=("scalable" "48x48" "64x64" "128x128" "256x256")

    for size in "${sizes[@]}"; do
        rm_safe "$icon_dir/$size/apps/$APP_NAME.svg"
        rm_safe "$icon_dir/$size/apps/$APP_NAME.png"
        rmdir_safe "$icon_dir/$size/apps"
        rmdir_safe "$icon_dir/$size"
    done

    rmdir_safe "$icon_dir"
}

# Remove MIME types
remove_mime_types() {
    step "Removing MIME types..."
    rm_safe "$PREFIX/share/mime/packages/$APP_NAME-sql.xml"

    if [[ "$DRY_RUN" == "false" ]] && command -v update-mime-database &>/dev/null; then
        update-mime-database "$PREFIX/share/mime" 2>/dev/null || true
    fi

    rmdir_safe "$PREFIX/share/mime/packages"
    rmdir_safe "$PREFIX/share/mime"
}

# Remove user configuration
remove_user_config() {
    if [[ "$REMOVE_CONFIG" == "false" ]]; then
        return
    fi

    step "Removing user configuration and data..."

    local config_dir="${XDG_CONFIG_HOME:-$HOME/.config}"
    local data_dir="${XDG_DATA_HOME:-$HOME/.local/share}"

    rm_rf_safe "$config_dir/$APP_NAME"
    rm_rf_safe "$data_dir/$APP_NAME"
}

# Main
main() {
    echo ""
    echo "  ╔══════════════════════════════════════╗"
    echo "  ║       DBFlux Linux Uninstaller       ║"
    echo "  ╚══════════════════════════════════════╝"
    echo ""

    check_prefix_writable

    # Find installed files
    local installed_files
    installed_files=$(find_installed_files)

    if [[ -z "$installed_files" ]]; then
        error "DBFlux is not installed at $PREFIX"
        echo ""
        echo "Try specifying a different prefix:"
        echo "  $0 --prefix ~/.local"
        echo "  $0 --prefix /usr"
        exit 1
    fi

    info "Found installed files:"
    for file in $installed_files; do
        echo "  - $file"
    done
    echo ""

    # Show user config if --remove-config
    if [[ "$REMOVE_CONFIG" == "true" ]]; then
        local user_config
        user_config=$(find_user_config)
        if [[ -n "$user_config" ]]; then
            warn "The following user data will also be removed:"
            for dir in $user_config; do
                echo "  - $dir"
            done
            echo ""
        fi
    fi

    # Confirm
    if [[ "$FORCE" == "false" ]] && [[ -t 0 ]]; then
        read -p "Proceed with uninstallation? [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            info "Uninstallation cancelled"
            exit 0
        fi
        echo ""
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY-RUN MODE: No changes will be made"
        echo ""
    fi

    remove_binary
    remove_desktop_entry
    remove_icons
    remove_mime_types
    remove_user_config

    echo ""
    if [[ "$DRY_RUN" == "false" ]]; then
        info "DBFlux uninstalled successfully!"
    else
        info "Dry-run completed. No changes were made."
    fi
}

main "$@"
