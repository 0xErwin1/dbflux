#!/usr/bin/env bash
#
# Rebuild the vendored gpui-pre copy from a published version.
#
# Usage:
#   vendor/gpui-pre/refresh.sh 0.3.6
#
# Downloads the crate, keeps the parts the patches need, re-applies
# element-transform.patch, frame-observer.patch, subscription-drop-log.patch,
# text-input-automation.patch and read-only-accessibility.patch in that order and leaves
# .rej files for hunks that no longer apply. See VENDOR.md for what to check afterwards.

set -euo pipefail

version="${1:-}"
if [[ -z "$version" ]]; then
    echo "usage: $0 <gpui-pre version>  (for example: $0 0.3.6)" >&2
    exit 1
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$here/../.." && pwd)"
crate="gpui-pre-$version"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "Fetching gpui-pre $version"
curl -fsSL "https://static.crates.io/crates/gpui-pre/$crate.crate" -o "$work/$crate.crate"
tar -xzf "$work/$crate.crate" -C "$work"
source_dir="$work/$crate"

if [[ ! -d "$source_dir/src" ]]; then
    echo "error: $crate.crate did not contain src/" >&2
    exit 1
fi

echo "Rebuilding $here"
rm -rf "$here/src" "$here/resources" "$here/Cargo.toml"
cp -r "$source_dir/src" "$here/src"
mkdir -p "$here/resources"
cp -r "$source_dir/resources/." "$here/resources/"
cp "$source_dir/build.rs" "$source_dir/LICENSE-APACHE" "$source_dir/README.md" "$here/"

# Drop the target tables whose sources are not vendored, then give the crate its own
# workspace root so Cargo does not expect it in DBFlux's member list, and record the
# cargo-machete exemption the published manifest does not carry.
awk '
    /^\[\[example\]\]/ { skip = 1 }
    /^\[\[test\]\]/    { skip = 1 }
    /^\[\[bench\]\]/   { skip = 1 }
    /^\[/ && !/^\[\[example\]\]|^\[\[test\]\]|^\[\[bench\]\]/ { skip = 0 }
    !skip { print }
' "$source_dir/Cargo.toml" > "$here/Cargo.toml"
printf '\n[workspace]\n' >> "$here/Cargo.toml"
cat >> "$here/Cargo.toml" <<'MANIFEST'

# cargo-machete, unlike cargo-shear above, has no exemption for this crate, and
# upstream's published source only reaches `tracing` through a cfg'd path that
# the heuristic does not follow. Keep the CI free of that false positive; the
# same crate is listed in the cargo-shear `ignored` list above for the same
# reason.
[package.metadata.cargo-machete]
ignored = ["tracing"]
MANIFEST

# Each patch is written against the tree the previous ones produce, so the order matters.
patches=(
    element-transform.patch
    frame-observer.patch
    subscription-drop-log.patch
    text-input-automation.patch
    read-only-accessibility.patch
)

cd "$repo_root"
tagged_rejects=()
for patch in "${patches[@]}"; do
    echo "Applying $patch"
    git apply -p3 --directory=vendor/gpui-pre --reject "vendor/gpui-pre/$patch" || true

    # git apply names every reject <file>.rej, so a later patch rejecting a hunk in the
    # same file would overwrite this one. Tag each new reject with the patch it came from.
    stem="${patch%.patch}"
    tagged_rejects+=(! -name "*.$stem.rej")
    while IFS= read -r -d '' reject; do
        mv "$reject" "${reject%.rej}.$stem.rej"
    done < <(find "$here" -name '*.rej' "${tagged_rejects[@]}" -print0)
done

rejects="$(find "$here" -name '*.rej' || true)"
if [[ -n "$rejects" ]]; then
    echo
    echo "Rejected hunks need a manual port (see VENDOR.md):"
    echo "$rejects"
    exit 2
fi

echo "Done. Bump the version constraints in the root Cargo.toml next."
