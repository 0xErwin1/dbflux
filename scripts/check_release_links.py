#!/usr/bin/env python3
"""Fail when the documentation links to a release asset the release does not ship.

The installation guide, its translations and the site's install section link to
`releases/latest/download/<name>`. That URL only resolves when the latest
release carries an asset with exactly that name, and nothing else checks it:
the link is valid markdown and the site builds, so a renamed artifact turns the
documented command into a 404 without any failure.

Usage:
    check_release_links.py ARTIFACTS_DIR

ARTIFACTS_DIR holds the files the release is about to publish (in CI, the
downloaded build artifacts). Every file name found under it, at any depth, is
an asset name. Run it before publishing, so a broken link stops the release
rather than shipping it.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]

LATEST_DOWNLOAD = re.compile(r"releases/latest/download/([A-Za-z0-9._+-]+)")

# InstallTabs.astro builds its download links from a `files: [...]` list
# joined to `releases/latest/download/`, so the names never appear in a URL.
FILES_LIST = re.compile(r"files:\s*\[([^\]]*)\]")
QUOTED = re.compile(r"""['"]([^'"]+)['"]""")


def documentation_files(root):
    """Files whose download links readers follow: the docs, every translation, the README and the site sources."""
    files = sorted((root / "docs").rglob("*.md"))
    files.append(root / "README.md")
    files.extend(sorted((root / "web" / "src").rglob("*.astro")))
    files.extend(sorted((root / "web" / "src").rglob("*.ts")))
    return [path for path in files if path.is_file()]


def linked_assets(path):
    """Return the (line number, asset name) pairs `path` links to."""
    text = path.read_text(encoding="utf-8")
    links = []

    for match in LATEST_DOWNLOAD.finditer(text):
        links.append((line_number(text, match.start(1)), match.group(1)))

    for files_match in FILES_LIST.finditer(text):
        body_start = files_match.start(1)
        for quoted in QUOTED.finditer(files_match.group(1)):
            links.append((line_number(text, body_start + quoted.start(1)), quoted.group(1)))

    return links


def line_number(text, offset):
    return text.count("\n", 0, offset) + 1


def published_assets(artifacts_dir):
    return {path.name for path in artifacts_dir.rglob("*") if path.is_file()}


def broken_links(root, assets):
    """Return (path, line, name) for every link whose asset is not published."""
    broken = []

    for path in documentation_files(root):
        for line, name in linked_assets(path):
            if name not in assets:
                broken.append((path.relative_to(root), line, name))

    return broken


def main(arguments):
    if len(arguments) != 1:
        print(__doc__.strip(), file=sys.stderr)
        return 2

    artifacts_dir = pathlib.Path(arguments[0])
    if not artifacts_dir.is_dir():
        print(f"error: {artifacts_dir} is not a directory", file=sys.stderr)
        return 2

    assets = published_assets(artifacts_dir)
    if not assets:
        print(f"error: no files under {artifacts_dir}", file=sys.stderr)
        return 2

    broken = broken_links(ROOT, assets)
    for path, line, name in broken:
        print(
            f"::error file={path},line={line}::{name} is linked through "
            "releases/latest/download/ but this release does not publish it"
        )

    if broken:
        print(f"{len(broken)} documentation link(s) point to assets this release does not publish.", file=sys.stderr)
        return 1

    print("Every releases/latest/download/ link resolves to a published asset.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
