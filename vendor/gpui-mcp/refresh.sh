#!/usr/bin/env bash
#
# Rebuild the vendored gpui-mcp crates from an upstream commit.
#
# Usage:
#   vendor/gpui-mcp/refresh.sh <full 40-character commit SHA>
#
# Downloads that commit of themixednuts/gpui-mcp, keeps the gpui-mcp,
# gpui-mcp-protocol, gpui-mcp-server and gpui-mcp-capture crates and the
# license, re-applies dbflux-port.patch, text-input-automation.patch and
# screenshot-freshness.patch in that order and leaves .rej files for hunks that
# no longer apply. See VENDOR.md for what to check afterwards.

set -euo pipefail

rev="${1:-}"
if [[ ! "$rev" =~ ^[0-9a-f]{40}$ ]]; then
    echo "usage: $0 <full 40-character commit SHA of themixednuts/gpui-mcp>" >&2
    exit 1
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$here/../.." && pwd)"
crates=(gpui-mcp gpui-mcp-protocol gpui-mcp-server gpui-mcp-capture)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "Fetching themixednuts/gpui-mcp@$rev"
curl -fsSL "https://codeload.github.com/themixednuts/gpui-mcp/tar.gz/$rev" -o "$work/gpui-mcp.tar.gz"
mkdir "$work/source"
tar -xzf "$work/gpui-mcp.tar.gz" -C "$work/source" --strip-components=1
source_dir="$work/source"

for crate in "${crates[@]}"; do
    if [[ ! -f "$source_dir/crates/$crate/Cargo.toml" ]]; then
        echo "error: upstream $rev has no crates/$crate" >&2
        exit 1
    fi
done

echo "Rebuilding $here"
rm -rf "$here/crates" "$here/LICENSE"
mkdir -p "$here/crates"
for crate in "${crates[@]}"; do
    cp -r "$source_dir/crates/$crate" "$here/crates/$crate"
done
cp "$source_dir/LICENSE" "$here/LICENSE"

# Each patch is written against the tree the previous ones produce.
patches=(dbflux-port.patch text-input-automation.patch screenshot-freshness.patch)

cd "$repo_root"
for patch in "${patches[@]}"; do
    echo "Applying $patch"
    git apply -p1 --directory=vendor/gpui-mcp --reject "vendor/gpui-mcp/$patch" || true
done

rejects="$(find "$here" -name '*.rej' || true)"
if [[ -n "$rejects" ]]; then
    echo
    echo "Rejected hunks need a manual port (see VENDOR.md):"
    echo "$rejects"
    exit 2
fi

echo "Done. Record $rev in VENDOR.md and run the checks it lists."
