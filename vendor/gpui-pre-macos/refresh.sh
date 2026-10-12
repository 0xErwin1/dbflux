#!/usr/bin/env bash
#
# Rebuild the vendored gpui-pre-macos copy from a published version.
#
# Usage:
#   vendor/gpui-pre-macos/refresh.sh 0.3.6
#
# Downloads the crate, keeps its sources, manifest and license, and re-applies
# release-accesskit-adapter.patch. A hunk that no longer applies is left as a .rej file.
# See VENDOR.md for what to check afterwards.

set -euo pipefail

version="${1:-}"
if [[ -z "$version" ]]; then
    echo "usage: $0 <gpui-pre-macos version>  (for example: $0 0.3.6)" >&2
    exit 1
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$here/../.." && pwd)"
crate="gpui-pre-macos-$version"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "Fetching gpui-pre-macos $version"
curl -fsSL "https://static.crates.io/crates/gpui-pre-macos/$crate.crate" -o "$work/$crate.crate"
tar -xzf "$work/$crate.crate" -C "$work"
source_dir="$work/$crate"

if [[ ! -d "$source_dir/src" ]]; then
    echo "error: $crate.crate did not contain src/" >&2
    exit 1
fi

echo "Rebuilding $here"
rm -rf "$here/src" "$here/Cargo.toml"
cp -r "$source_dir/src" "$here/src"
cp "$source_dir/Cargo.toml" "$source_dir/LICENSE-APACHE" "$here/"
# Its own workspace root, so Cargo does not expect it in DBFlux's member list.
printf '\n[workspace]\n' >> "$here/Cargo.toml"

cd "$repo_root"
echo "Applying release-accesskit-adapter.patch"
git apply -p1 --directory=vendor/gpui-pre-macos --reject \
    "vendor/gpui-pre-macos/release-accesskit-adapter.patch" || true

rejects="$(find "$here" -name '*.rej' || true)"
if [[ -n "$rejects" ]]; then
    echo
    echo "Rejected hunks need a manual port (see VENDOR.md):"
    echo "$rejects"
    exit 2
fi

echo "Done. Bump the gpui-pre-macos version in Cargo.lock next (cargo update -p gpui-pre-macos)."
