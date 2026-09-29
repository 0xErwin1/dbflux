#!/usr/bin/env python3
"""Run rustfmt, clippy, or the static source checks on the first-party crates of DBFlux.

First-party crates are the workspace packages whose manifest lives under
`<repo>/crates/`. The crates under `vendor/` are third-party code kept in the
workspace, so this script never lints or reformats them. CI and local
development both call this script, which keeps that selection in one place.

Usage:
    scripts/lint.py fmt [--check]
    scripts/lint.py clippy [extra cargo arguments...]
    scripts/lint.py mouse-down

Arguments given to `clippy` are passed to cargo verbatim before `--`, for
example `--locked --features "sqlite,postgres"` or `--tests`.

`mouse-down` fails on every left-button `on_mouse_down` handler in non-test code
under `crates/` that is not listed in `scripts/mouse_down_allowlist.txt`.
Activations belong in `on_click`, which runs on release and is exposed to
assistive technology as a click action; `on_mouse_down` is reserved for press
semantics such as drag starts, resize grips and focus guards.
"""

import argparse
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MOUSE_DOWN_ALLOWLIST = ROOT / "scripts" / "mouse_down_allowlist.txt"
LEFT_MOUSE_DOWN = re.compile(r"\.on_mouse_down\(\s*(?:gpui::)?MouseButton::Left\b")
FUNCTION_HEADER = re.compile(r"^(\s*)(?:pub(?:\([^)]*\))?\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+(\w+)")
TEST_MODULE_HEADER = re.compile(r"^(\s*)(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{")


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


def is_test_path(path: Path) -> bool:
    """Whether a source file only holds tests: a `tests/` directory or a `tests.rs` module."""
    relative = path.relative_to(ROOT)

    return "tests" in relative.parts[:-1] or path.stem == "tests" or path.stem.endswith("_tests")


def test_module_lines(lines: list[str]) -> set[int]:
    """Return the zero-based line numbers that sit inside `#[cfg(test)] mod name { ... }` blocks.

    rustfmt closes a module with a `}` at the indentation of its `mod` line, so
    the block ends at the first such line after the header.
    """
    inside: set[int] = set()
    index = 0

    while index < len(lines):
        if lines[index].strip() != "#[cfg(test)]":
            index += 1
            continue

        header_index = index + 1
        while header_index < len(lines) and lines[header_index].strip().startswith("#["):
            header_index += 1

        header = TEST_MODULE_HEADER.match(lines[header_index]) if header_index < len(lines) else None
        if header is None:
            index += 1
            continue

        closing = header.group(1) + "}"
        end_index = header_index + 1
        while end_index < len(lines) and lines[end_index].rstrip() != closing:
            end_index += 1

        inside.update(range(index, end_index + 1))
        index = end_index + 1

    return inside


def enclosing_function(lines: list[str], line_index: int) -> str:
    """Name the innermost `fn` whose header is less indented than the handler line."""
    site_indent = len(lines[line_index]) - len(lines[line_index].lstrip())

    for index in range(line_index, -1, -1):
        header = FUNCTION_HEADER.match(lines[index])
        if header and len(header.group(1)) < site_indent:
            return header.group(2)

    return "<module>"


def left_mouse_down_sites() -> list[tuple[str, str, int]]:
    """Find every non-test left-button `on_mouse_down` in `crates/` as (path, function, line)."""
    sites = []

    for path in sorted((ROOT / "crates").rglob("*.rs")):
        if is_test_path(path):
            continue

        text = path.read_text(encoding="utf-8")
        if "on_mouse_down" not in text:
            continue

        lines = text.split("\n")
        excluded = test_module_lines(lines)
        relative = path.relative_to(ROOT).as_posix()

        for match in LEFT_MOUSE_DOWN.finditer(text):
            line_index = text.count("\n", 0, match.start())
            if line_index in excluded:
                continue

            sites.append((relative, enclosing_function(lines, line_index), line_index + 1))

    return sites


def read_mouse_down_allowlist() -> dict[tuple[str, str], int] | None:
    """Parse `path:function: reason` entries into a count per (path, function).

    One entry covers one handler, so a function with three press handlers is
    listed three times. Returns None after printing the offending lines when
    an entry is malformed.
    """
    allowed: dict[tuple[str, str], int] = {}
    malformed = []

    if not MOUSE_DOWN_ALLOWLIST.exists():
        return allowed

    for number, raw in enumerate(MOUSE_DOWN_ALLOWLIST.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        parts = [part.strip() for part in line.split(":", 2)]
        if len(parts) != 3 or not all(parts):
            malformed.append(f"{MOUSE_DOWN_ALLOWLIST.name}:{number}: expected `path:function: reason`")
            continue

        key = (parts[0], parts[1])
        allowed[key] = allowed.get(key, 0) + 1

    if malformed:
        print("\n".join(malformed), file=sys.stderr)
        return None

    return allowed


def check_mouse_down() -> int:
    """Fail on left-button `on_mouse_down` handlers missing from the allowlist, and on stale entries."""
    allowed = read_mouse_down_allowlist()
    if allowed is None:
        return 1

    sites = left_mouse_down_sites()
    found: dict[tuple[str, str], list[int]] = {}
    for path, function, line in sites:
        found.setdefault((path, function), []).append(line)

    errors = []
    for key, lines in sorted(found.items()):
        missing = len(lines) - allowed.get(key, 0)
        if missing > 0:
            locations = ", ".join(str(line) for line in lines)
            errors.append(
                f"{key[0]}:{key[1]}: {missing} unlisted left-button on_mouse_down "
                f"(lines {locations}); use on_click for activations, or allowlist a press handler"
            )

    for key, count in sorted(allowed.items()):
        stale = count - len(found.get(key, []))
        if stale > 0:
            errors.append(f"{key[0]}:{key[1]}: {stale} stale allowlist entr{'y' if stale == 1 else 'ies'}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        print(f"mouse-down: {len(errors)} problem(s), {len(sites)} handler(s) found", file=sys.stderr)
        return 1

    print(f"mouse-down: {len(sites)} allowlisted press handler(s), no unlisted activations")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run rustfmt, clippy, or static checks on first-party crates only (never vendor/)."
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    fmt_parser = subcommands.add_parser("fmt", help="Format first-party crates with rustfmt")
    fmt_parser.add_argument(
        "--check", action="store_true", help="Report formatting differences without writing"
    )

    subcommands.add_parser(
        "clippy", help="Lint first-party crates with clippy; extra arguments go to cargo"
    )

    subcommands.add_parser(
        "mouse-down",
        help="Fail on left-button on_mouse_down handlers missing from scripts/mouse_down_allowlist.txt",
    )

    arguments, extra = parser.parse_known_args()

    if arguments.command in ("fmt", "mouse-down") and extra:
        parser.error(f"unrecognized arguments: {' '.join(extra)}")

    if arguments.command == "mouse-down":
        return check_mouse_down()

    packages = package_arguments(first_party_packages())

    if arguments.command == "fmt":
        command = ["cargo", "fmt", *packages]
        if arguments.check:
            command += ["--", "--check"]

        return run(command)

    return run(["cargo", "clippy", "--no-deps", *packages, *extra, "--", "-D", "warnings"])


if __name__ == "__main__":
    sys.exit(main())
