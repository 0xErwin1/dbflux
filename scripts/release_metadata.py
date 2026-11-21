#!/usr/bin/env python3
"""Update Nix and site pointers from a published stable/RC release.

Usage: release_metadata.py --tag vX.Y.Z[-rc.N] --checksums DIR --root CHECKOUT
The caller validates publication, repository and release-branch ancestry.
Each pointer advances independently; equal versions are byte-preserving no-ops.
Only nix/release-info.nix and web/versions.json in CHECKOUT are written.
"""
import argparse
import base64
import json
import pathlib
import re
import sys

from homebrew_cask import RenderError, read_checksum

VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.(0|[1-9][0-9]*))?")


def version_key(version):
    match = VERSION.fullmatch(version)
    if not match:
        raise RenderError("expected vX.Y.Z or vX.Y.Z-rc.N")
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), rc is None, int(rc or 0))


def render_nix(version, checksums):
    lines = ["{", f'  version = "{version}";', "  artifacts = {"]
    for system, arch in (("x86_64-linux", "amd64"), ("aarch64-linux", "arm64")):
        asset = f"dbflux-linux-{arch}.tar.gz"
        digest = read_checksum(checksums, asset)
        sri = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
        lines.extend([
            f'    "{system}" = {{',
            f'      url = "https://github.com/0xErwin1/dbflux/releases/download/v{version}/{asset}";',
            f'      hash = "sha256-{sri}";',
            "    };",
        ])
    return "\n".join(lines + ["  };", "}", ""])


def update(tag, root, checksums):
    if not tag.startswith("v"):
        raise RenderError("expected vX.Y.Z or vX.Y.Z-rc.N")
    version = tag[1:]
    incoming = version_key(version)
    pointer = root / "nix/release-info.nix"
    original = pointer.read_text(encoding="utf-8")
    match = re.search(r'^\s*version = "([^\"]+)";', original, re.MULTILINE)
    if not match:
        raise RenderError("cannot read Nix release version")
    nix_content = render_nix(version, checksums) if incoming > version_key(match[1]) else original

    registry = root / "web/versions.json"
    site_original = registry.read_text(encoding="utf-8")
    entries = json.loads(site_original)
    minor = incoming[:2]
    identifier = f"v{minor[0]}.{minor[1]}"
    def minor_key(entry):
        return tuple(map(int, entry["id"][1:].split(".")))

    # Older-line patches must not demote the unprefixed documentation URL.
    current = [minor_key(entry) for entry in entries if entry.get("current")]
    changed = False
    if not any(entry["id"] == identifier for entry in entries):
        entries.append({"id": identifier, "ref": f"release/{identifier}"})
        entries.sort(key=lambda entry: (entry["id"] == "nightly", () if entry["id"] == "nightly" else minor_key(entry)), reverse=True)
        changed = True
    if incoming[3] and (not current or minor >= max(current)):
        for entry in entries:
            desired = entry["id"] == identifier
            if bool(entry.get("current")) != desired:
                changed = True
                if desired:
                    entry["current"] = True
                else:
                    entry.pop("current", None)
    site_content = json.dumps(entries, indent=2) + "\n" if changed else site_original
    # Validate and render both outputs before touching either pointer.
    if nix_content != original:
        pointer.write_text(nix_content, encoding="utf-8")
    if site_content != site_original:
        registry.write_text(site_content, encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="published vX.Y.Z or vX.Y.Z-rc.N tag")
    parser.add_argument("--checksums", required=True, type=pathlib.Path, help="published Linux tarball .sha256 directory")
    parser.add_argument("--root", required=True, type=pathlib.Path, help="target branch checkout")
    args = parser.parse_args(argv)
    try:
        update(args.tag, args.root, args.checksums)
    except (RenderError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
