#!/usr/bin/env python3
"""Render the DBFlux Homebrew cask for a stable release.

Reads the `<asset>.sha256` files the release publishes next to each asset and
writes `Casks/dbflux.rb` for the tap repository `0xErwin1/homebrew-dbflux`.
The cask covers macOS (the DMG) and Linux (the tarball's binary) so the tap
needs no separate formula with the same name.

Usage:
    homebrew_cask.py --version 0.9.0 --checksums DIR --output Casks/dbflux.rb
"""
import argparse
import pathlib
import re
import sys

STABLE_VERSION = re.compile(r"\d+\.\d+\.\d+")
SHA256 = re.compile(r"[0-9a-f]{64}")

# Cask `sha256` slot for each release asset.
ASSETS = (
    ("arm", "dbflux-macos-arm64.dmg"),
    ("intel", "dbflux-macos-amd64.dmg"),
    ("arm64_linux", "dbflux-linux-arm64.tar.gz"),
    ("x86_64_linux", "dbflux-linux-amd64.tar.gz"),
)

TEMPLATE = """\
cask "dbflux" do
  arch arm: "arm64", intel: "amd64"

  version "{version}"
  sha256 arm:          "{arm}",
         intel:        "{intel}",
         arm64_linux:  "{arm64_linux}",
         x86_64_linux: "{x86_64_linux}"

  name "DBFlux"
  desc "Keyboard-first database client"
  homepage "https://dbflux.dev"

  livecheck do
    url "https://github.com/0xErwin1/dbflux"
    strategy :github_latest
  end

  on_macos do
    url "https://github.com/0xErwin1/dbflux/releases/download/v#{{version}}/dbflux-macos-#{{arch}}.dmg"

    app "DBFlux.app"

    # The app is not notarized, so Gatekeeper would refuse to open it after
    # every install and upgrade.
    postflight do
      system_command "/usr/bin/xattr",
                     args: ["-dr", "com.apple.quarantine", "#{{appdir}}/DBFlux.app"]
    end

    zap trash: "~/Library/Application Support/dbflux"
  end

  on_linux do
    url "https://github.com/0xErwin1/dbflux/releases/download/v#{{version}}/dbflux-linux-#{{arch}}.tar.gz"

    binary "dbflux"

    zap trash: "~/.local/share/dbflux"
  end
end
"""


class RenderError(Exception):
    pass


def read_checksum(directory, asset):
    path = directory / f"{asset}.sha256"
    try:
        content = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise RenderError(f"missing {path}") from None

    fields = content.split()
    if not fields or not SHA256.fullmatch(fields[0]):
        raise RenderError(f"{path.name} does not hold a SHA-256 hex digest")

    named = fields[1].lstrip("*") if len(fields) > 1 else asset
    if named != asset:
        raise RenderError(f"{path.name} names {named}, expected {asset}")

    return fields[0]


def render(version, checksum_dir):
    if not STABLE_VERSION.fullmatch(version):
        raise RenderError(f"version '{version}' is not a stable X.Y.Z release")

    hashes = {slot: read_checksum(checksum_dir, asset) for slot, asset in ASSETS}
    return TEMPLATE.format(version=version, **hashes)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True, help="stable version, without the leading v")
    parser.add_argument("--checksums", required=True, type=pathlib.Path, help="directory with the .sha256 files")
    parser.add_argument("--output", required=True, type=pathlib.Path, help="cask file to write")
    args = parser.parse_args(argv)

    try:
        cask = render(args.version, args.checksums)
    except RenderError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(cask, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
