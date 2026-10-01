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
UNINSTALL_SCRIPT_URL="https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh"
APP_NAME="dbflux"
DEFAULT_PREFIX="/usr/local"

ORIGINAL_ARGS=("$@")

# A script read from a pipe (`curl | bash`) has no regular file behind BASH_SOURCE.
if [[ -f "${BASH_SOURCE[0]:-}" ]]; then
    REMOTE_MODE=false
else
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

# Print the command that re-runs this uninstaller the way it was started
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
            echo "curl -fsSL $UNINSTALL_SCRIPT_URL | ${sudo_prefix}bash -s --$arguments"
        else
            echo "curl -fsSL $UNINSTALL_SCRIPT_URL | ${sudo_prefix}bash"
        fi
    else
        echo "${sudo_prefix}$0$arguments"
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
        echo "  2. Specify correct prefix: $(rerun_command "" --prefix "$HOME/.local")" >&2
        exit 1
    fi
}

# Collect installed files into INSTALLED_FILES.
find_installed_files() {
    local -n files=INSTALLED_FILES

    [[ -f "$PREFIX/bin/$APP_NAME" ]] && files+=("$PREFIX/bin/$APP_NAME")
    [[ -f "$PREFIX/share/applications/$APP_NAME.desktop" ]] && files+=("$PREFIX/share/applications/$APP_NAME.desktop")
    [[ -f "$PREFIX/share/icons/hicolor/scalable/apps/$APP_NAME.svg" ]] && files+=("$PREFIX/share/icons/hicolor/scalable/apps/$APP_NAME.svg")
    [[ -f "$PREFIX/share/mime/packages/$APP_NAME-sql.xml" ]] && files+=("$PREFIX/share/mime/packages/$APP_NAME-sql.xml")

    # Check other icon sizes
    for size in 48x48 64x64 128x128 256x256; do
        [[ -f "$PREFIX/share/icons/hicolor/$size/apps/$APP_NAME.svg" ]] && files+=("$PREFIX/share/icons/hicolor/$size/apps/$APP_NAME.svg")
        [[ -f "$PREFIX/share/icons/hicolor/$size/apps/$APP_NAME.png" ]] && files+=("$PREFIX/share/icons/hicolor/$size/apps/$APP_NAME.png")
    done

    return 0
}

# Collect existing user config and data directories into USER_CONFIG_DIRS.
find_user_config() {
    local -n dirs=USER_CONFIG_DIRS

    local config_dir="${XDG_CONFIG_HOME:-$HOME/.config}"
    local data_dir="${XDG_DATA_HOME:-$HOME/.local/share}"

    [[ -d "$config_dir/$APP_NAME" ]] && dirs+=("$config_dir/$APP_NAME")
    [[ -d "$data_dir/$APP_NAME" ]] && dirs+=("$data_dir/$APP_NAME")

    return 0
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

    INSTALLED_FILES=()
    find_installed_files

    if [[ ${#INSTALLED_FILES[@]} -eq 0 ]]; then
        error "DBFlux is not installed at $PREFIX"
        echo ""
        echo "Try specifying a different prefix:"
        echo "  $0 --prefix ~/.local"
        echo "  $0 --prefix /usr"
        exit 1
    fi

    info "Found installed files:"
    for file in "${INSTALLED_FILES[@]}"; do
        echo "  - $file"
    done
    echo ""

    # Show user config if --remove-config
    if [[ "$REMOVE_CONFIG" == "true" ]]; then
        # sudo resets HOME to root's on most distributions, so the directories
        # below belong to root, not to the user who invoked sudo.
        if [[ $EUID -eq 0 ]] && [[ -n "${SUDO_USER:-}" ]]; then
            warn "Running under sudo: --remove-config targets root's directories, not $SUDO_USER's"
            warn "To remove $SUDO_USER's data, run as $SUDO_USER: rm -rf ~/.config/$APP_NAME ~/.local/share/$APP_NAME"
        fi

        USER_CONFIG_DIRS=()
        find_user_config
        if [[ ${#USER_CONFIG_DIRS[@]} -gt 0 ]]; then
            warn "The following user data will also be removed:"
            for dir in "${USER_CONFIG_DIRS[@]}"; do
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
