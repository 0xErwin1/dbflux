#!/usr/bin/env python3
"""Run rustfmt or clippy on the first-party crates of the DBFlux workspace.

First-party crates are the workspace packages whose manifest lives under
`<repo>/crates/`. The crates under `vendor/` are third-party code kept in the
workspace, so this script never lints or reformats them. CI and local
development both call this script, which keeps that selection in one place.

Usage:
    scripts/lint.py fmt [--check]
    scripts/lint.py clippy [extra cargo arguments...]

Arguments given to `clippy` are passed to cargo verbatim before `--`, for
example `--locked --features "sqlite,postgres"` or `--tests`.
"""

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def first_party_packages() -> list[str]:
    """Return the sorted names of workspace packages whose manifest is under `crates/`.

    Exits with an error when none are found, so a broken metadata query can
    never turn into a lint run that silently checks nothing.
    """
    metadata = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    crates_dir = (ROOT / "crates").resolve()
    packages = sorted(
        package["name"]
        for package in json.loads(metadata.stdout)["packages"]
        if Path(package["manifest_path"]).resolve().is_relative_to(crates_dir)
    )

    if not packages:
        sys.exit("No first-party crates found under crates/; refusing to lint nothing")

    return packages


def package_arguments(packages: list[str]) -> list[str]:
    """Expand package names into repeated `--package <name>` cargo arguments."""
    return [argument for name in packages for argument in ("--package", name)]


def run(command: list[str]) -> int:
    """Print the command to stderr, run it from the repository root, and return its exit code."""
    print(shlex.join(command), file=sys.stderr, flush=True)

    return subprocess.run(command, cwd=ROOT).returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run rustfmt or clippy on first-party crates only (never vendor/)."
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    fmt_parser = subcommands.add_parser("fmt", help="Format first-party crates with rustfmt")
    fmt_parser.add_argument(
        "--check", action="store_true", help="Report formatting differences without writing"
    )

    subcommands.add_parser(
        "clippy", help="Lint first-party crates with clippy; extra arguments go to cargo"
    )

    arguments, extra = parser.parse_known_args()

    if arguments.command == "fmt" and extra:
        parser.error(f"unrecognized arguments: {' '.join(extra)}")

    packages = package_arguments(first_party_packages())

    if arguments.command == "fmt":
        command = ["cargo", "fmt", *packages]
        if arguments.check:
            command += ["--", "--check"]

        return run(command)

    return run(["cargo", "clippy", "--no-deps", *packages, *extra, "--", "-D", "warnings"])


if __name__ == "__main__":
    sys.exit(main())
