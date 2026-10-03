#!/usr/bin/env python3
"""Regenerate the DBFlux documentation screenshots.

The script starts demo databases in Docker, runs DBFlux on a headless X server
(Xvfb with Mesa's software Vulkan driver), drives it through the UI automation
MCP server (`gpui-mcp`) and writes one WebP per shot and theme to
`<out>/<page>/<name>-<theme>.webp`.

The shots are declared in `scripts/docs_screenshots/shots.py`. A setup run
creates the demo connections through the Connection Manager and picks the
theme in Settings, and the resulting DBFlux profile is saved. Every shot then
runs in a fresh DBFlux process started from a copy of that profile, so a shot
looks the same whether it runs alone (`--only`) or with the others.

Run it from the Nix dev shell, which provides Xvfb, the Vulkan driver, xdotool
and cwebp:

    nix develop -c python3 scripts/docs_screenshots.py --out /tmp/shots

See docs/UI_AUTOMATION.md, "Regenerating documentation screenshots".
"""

from __future__ import annotations

import argparse
import base64
import ctypes
import json
import os
import re
import select
import shlex
import shutil
import signal
import sqlite3
import struct
import subprocess
import sys
import time
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SHOTS_DIR = REPO_ROOT / "scripts" / "docs_screenshots"
SEED_DIR = SHOTS_DIR / "seed"

sys.path.insert(0, str(SHOTS_DIR))

import shots as shot_list  # noqa: E402

CONTAINER_PREFIX = "dbflux-docs-"
XVFB_SCREEN = "3840x2160x24"
UI_SCALE = 2
WINDOW_WIDTH = 1600
WINDOW_HEIGHT = 900
THEMES = ("light", "dark")
PROFILE_DIRS = ("data", "config", "state", "cache")

# DBFlux shows the path of a script and of a SQLite file, so the work
# directory has a fixed, short path to keep the images stable between runs.
DEFAULT_WORK_DIR = Path("/tmp/dbflux-docs")

WEBP_QUALITY = 80
DESCRIPTOR_TIMEOUT_SECONDS = 120
SERVICE_READY_TIMEOUT_SECONDS = 120
WINDOW_TIMEOUT_SECONDS = 60
SCREENSHOT_ATTEMPTS = 10
SCREENSHOT_RETRY_SECONDS = 2
KEY_ATTEMPTS = 3
KEY_RETRY_SECONDS = 15
ENSURE_QUICK_CHECK_MS = 1_000
COMMAND_TIMEOUT_SECONDS = 300
# `docker run` may pull an image first.
CONTAINER_START_TIMEOUT_SECONDS = 1_200
WORK_DIR_MARKER = ".dbflux-docs-work-dir"
ENSURE_CHECK_MS = 15_000

DESCRIPTOR_PATTERN = re.compile(r"descriptor (?:\x1b\[[0-9;]*m)*(\S+\.json)")
TIMEOUT_ERROR_PREFIX = "Timeout:"
CLOSED_CONNECTION_ERROR = "closed the connection"


class ScreenshotError(RuntimeError):
    """A failure the user can act on, reported without a traceback."""


# ---------------------------------------------------------------------------
# Demo databases
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DemoService:
    """One demo database container, seeded from a file under `seed/`."""

    name: str
    image: str
    host_port: int
    container_port: int
    seed_file: str
    ready_command: tuple[str, ...]
    seed_command: tuple[str, ...]
    environment: dict[str, str] = field(default_factory=dict)

    @property
    def container(self) -> str:
        return f"{CONTAINER_PREFIX}{self.name}"


# Fixed host ports keep the connection details, and therefore the images, the
# same between runs. They are far from the default ports, so a database server
# already running on the machine does not collide with them.
DEMO_SERVICES = (
    DemoService(
        name="postgres",
        image="postgres:16-alpine",
        host_port=55432,
        container_port=5432,
        seed_file="postgres.sql",
        environment={"POSTGRES_HOST_AUTH_METHOD": "trust", "POSTGRES_DB": "shop"},
        # The image's init phase runs a server that only listens on the Unix
        # socket, so a TCP probe succeeds only once the real server is up.
        ready_command=("pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "shop"),
        seed_command=("psql", "-v", "ON_ERROR_STOP=1", "-q", "-U", "postgres", "-d", "shop", "-f"),
    ),
    DemoService(
        name="mongodb",
        image="mongo:7",
        host_port=57017,
        container_port=27017,
        seed_file="mongo.js",
        ready_command=("mongosh", "--quiet", "--eval", "db.runCommand({ ping: 1 }).ok"),
        seed_command=("mongosh", "--quiet", "shop"),
    ),
    DemoService(
        name="redis",
        image="redis:7-alpine",
        host_port=56379,
        container_port=6379,
        seed_file="redis.txt",
        ready_command=("redis-cli", "ping"),
        seed_command=("sh", "-c", 'redis-cli < "$0"'),
    ),
)


def run_command(
    command: list[str],
    *,
    check: bool = True,
    timeout: float = COMMAND_TIMEOUT_SECONDS,
    **kwargs,
) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(command, check=check, text=True, capture_output=True, timeout=timeout, **kwargs)
    except subprocess.TimeoutExpired as error:
        raise ScreenshotError(f"{' '.join(command[:3])} did not finish within {timeout:.0f} s") from error


def remove_demo_containers() -> None:
    listing = run_command(["docker", "ps", "-aq", "--filter", f"name=^{CONTAINER_PREFIX}"], check=False)
    container_ids = listing.stdout.split()

    if container_ids:
        run_command(["docker", "rm", "-f", *container_ids], check=False)


def start_demo_service(service: DemoService) -> None:
    command = ["docker", "run", "-d", "--name", service.container]
    command += ["-p", f"127.0.0.1:{service.host_port}:{service.container_port}"]

    for key, value in service.environment.items():
        command += ["-e", f"{key}={value}"]

    result = run_command([*command, service.image], check=False, timeout=CONTAINER_START_TIMEOUT_SECONDS)

    if result.returncode != 0:
        raise ScreenshotError(
            f"could not start the {service.name} container ({service.image}) on "
            f"127.0.0.1:{service.host_port}: {result.stderr.strip()}"
        )


def wait_for_demo_service(service: DemoService) -> None:
    deadline = time.monotonic() + SERVICE_READY_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        probe = run_command(["docker", "exec", service.container, *service.ready_command], check=False)

        if probe.returncode == 0:
            return

        time.sleep(1)

    raise ScreenshotError(
        f"the {service.name} container did not become ready within {SERVICE_READY_TIMEOUT_SECONDS} s"
    )


def seed_demo_service(service: DemoService) -> None:
    container_path = f"/tmp/{service.seed_file}"
    run_command(["docker", "cp", str(SEED_DIR / service.seed_file), f"{service.container}:{container_path}"])

    result = run_command(["docker", "exec", service.container, *service.seed_command, container_path], check=False)

    # redis-cli reports a failed command on stdout and still exits with 0.
    output_lines = result.stdout.splitlines()
    failed = result.returncode != 0 or any(line.startswith(("ERR", "(error)")) for line in output_lines)

    if failed:
        raise ScreenshotError(
            f"seeding {service.name} from seed/{service.seed_file} failed:\n"
            f"{result.stdout.strip()}\n{result.stderr.strip()}"
        )


def start_demo_databases() -> None:
    remove_demo_containers()

    for service in DEMO_SERVICES:
        start_demo_service(service)

    for service in DEMO_SERVICES:
        log(f"seeding {service.name}")
        wait_for_demo_service(service)
        seed_demo_service(service)


def create_sqlite_database(path: Path) -> None:
    path.unlink(missing_ok=True)

    with sqlite3.connect(path) as connection:
        connection.executescript((SEED_DIR / "sqlite.sql").read_text())

    connection.close()


# ---------------------------------------------------------------------------
# Headless display
# ---------------------------------------------------------------------------


def start_xvfb(log_path: Path) -> tuple[subprocess.Popen, str]:
    """Starts Xvfb on the first free display and returns it with `:<n>`."""

    read_end, write_end = os.pipe()

    # Xvfb exits when it cannot write the display number to the pipe, so the
    # pipe stays open until the whole line is read, and -noreset keeps it from
    # writing again when its last client disconnects between two DBFlux runs.
    command = ["Xvfb", "-displayfd", str(write_end), "-screen", "0", XVFB_SCREEN, "-nolisten", "tcp", "-noreset"]

    with log_path.open("w") as log_file:
        process = subprocess.Popen(
            command,
            pass_fds=(write_end,),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    os.close(write_end)

    output = b""
    deadline = time.monotonic() + 30

    while not output.endswith(b"\n") and time.monotonic() < deadline:
        ready, _, _ = select.select([read_end], [], [], max(deadline - time.monotonic(), 0))
        chunk = os.read(read_end, 64) if ready else b""

        if not chunk:
            break

        output += chunk

    os.close(read_end)
    number = output.decode().strip() if output.endswith(b"\n") else ""

    if not number:
        stop_process(process)
        raise ScreenshotError(f"Xvfb did not report a display; see {log_path}")

    return process, f":{number}"


def xdotool(display: str, *arguments: str) -> subprocess.CompletedProcess:
    return run_command(["xdotool", *arguments], check=False, env={**os.environ, "DISPLAY": display})


def visible_windows(display: str, pid: int) -> list[int]:
    search = xdotool(display, "search", "--onlyvisible", "--pid", str(pid))

    return [int(window_id) for window_id in search.stdout.split()]


def publish_client_list(display: str, window_ids: list[int]) -> None:
    """Lists `window_ids` in the root window's `_NET_CLIENT_LIST_STACKING`.

    gpui-mcp finds the window to capture through that property, which a
    window manager maintains. Xvfb runs without one, so the script sets it.
    """

    try:
        libx11 = ctypes.CDLL("libX11.so.6")
    except OSError as error:
        raise ScreenshotError(f"libX11 could not be loaded ({error}); run inside `nix develop`") from error

    libx11.XOpenDisplay.restype = ctypes.c_void_p
    libx11.XOpenDisplay.argtypes = [ctypes.c_char_p]
    libx11.XDefaultRootWindow.restype = ctypes.c_ulong
    libx11.XDefaultRootWindow.argtypes = [ctypes.c_void_p]
    libx11.XInternAtom.restype = ctypes.c_ulong
    libx11.XInternAtom.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int]
    libx11.XChangeProperty.argtypes = [
        ctypes.c_void_p,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    libx11.XCloseDisplay.argtypes = [ctypes.c_void_p]

    handle = libx11.XOpenDisplay(display.encode())

    if not handle:
        raise ScreenshotError(f"could not open the X display {display}")

    atom_window = 33
    prop_mode_replace = 0
    root = libx11.XDefaultRootWindow(handle)
    client_list = libx11.XInternAtom(handle, b"_NET_CLIENT_LIST_STACKING", 0)
    # Xlib passes 32-bit property items as C longs.
    values = (ctypes.c_ulong * len(window_ids))(*window_ids)

    libx11.XChangeProperty(handle, root, client_list, atom_window, 32, prop_mode_replace, values, len(window_ids))
    libx11.XCloseDisplay(handle)


def place_main_window(display: str, pid: int) -> None:
    """Moves the main window to the screen origin and gives it a fixed size.

    DBFlux sizes its first window from the screen, and Xvfb has no window
    manager, so the window is moved and resized directly.
    """

    deadline = time.monotonic() + WINDOW_TIMEOUT_SECONDS

    while True:
        search = xdotool(display, "search", "--all", "--onlyvisible", "--pid", str(pid), "--name", "^DBFlux$")
        window_ids = search.stdout.split()

        if window_ids:
            break

        if time.monotonic() > deadline:
            raise ScreenshotError("the DBFlux main window did not appear on the headless display")

        time.sleep(0.5)

    xdotool(display, "windowmove", window_ids[0], "0", "0")
    xdotool(display, "windowsize", window_ids[0], str(WINDOW_WIDTH * UI_SCALE), str(WINDOW_HEIGHT * UI_SCALE))


def wait_for_window_layout(session: McpSession) -> None:
    """Waits until the element tree describes the resized window.

    Until DBFlux draws a frame at the new size the tree keeps the old bounds,
    and a click aimed at an element there lands somewhere else.
    """

    deadline = time.monotonic() + WINDOW_TIMEOUT_SECONDS

    while True:
        widest = max(
            (
                (node["bounds"]["width"], node["bounds"]["height"])
                for node in session.elements().values()
                if node.get("bounds")
            ),
            default=(0, 0),
        )

        if widest == (WINDOW_WIDTH, WINDOW_HEIGHT):
            return

        if time.monotonic() > deadline:
            raise ScreenshotError(f"the window layout is {widest[0]}x{widest[1]}, not {WINDOW_WIDTH}x{WINDOW_HEIGHT}")

        time.sleep(0.5)


# ---------------------------------------------------------------------------
# MCP client
# ---------------------------------------------------------------------------


class ToolError(RuntimeError):
    def __init__(self, tool: str, message: str):
        super().__init__(f"{tool}: {message}")
        self.tool = tool
        self.message = message

    @property
    def is_timeout(self) -> bool:
        return self.message.startswith(TIMEOUT_ERROR_PREFIX)


class McpSession:
    """A `gpui-mcp` server pinned to one window descriptor, spoken to over stdio."""

    def __init__(self, command: list[str], descriptor: Path, environment: dict, log_path: Path):
        self.descriptor = descriptor
        self.next_id = 1
        self.log_file = log_path.open("a")
        self.process = subprocess.Popen(
            [*command, "--endpoint", str(descriptor)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log_file,
            env=environment,
            text=True,
            bufsize=1,
        )

        self.request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "dbflux-docs-screenshots", "version": "1"},
            },
        )
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})

    def send(self, message: dict) -> None:
        if self.process.stdin is None or self.process.poll() is not None:
            raise ScreenshotError("the gpui-mcp server exited; see gpui-mcp.log in the work directory")

        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def request(self, method: str, params: dict) -> dict:
        request_id = self.next_id
        self.next_id += 1
        self.send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})

        while True:
            line = self.process.stdout.readline() if self.process.stdout else ""

            if not line:
                raise ScreenshotError("the gpui-mcp server closed its output; see gpui-mcp.log in the work directory")

            message = json.loads(line)

            if message.get("id") != request_id:
                continue

            if "error" in message:
                raise ToolError(method, message["error"].get("message", str(message["error"])))

            return message["result"]

    def call(self, tool: str, arguments: dict | None = None) -> dict:
        result = self.request("tools/call", {"name": tool, "arguments": arguments or {}})
        text = " ".join(block.get("text", "") for block in result.get("content", []) if block.get("type") == "text")

        if result.get("isError"):
            raise ToolError(tool, text.strip())

        if "structuredContent" not in result and text.startswith("{"):
            result["structuredContent"] = json.loads(text)

        return result

    def find(self, label: str, *, exact: bool = True) -> list[dict]:
        result = self.call("find_elements", {"query": label, "exact": exact})

        return result.get("structuredContent", {}).get("elements", [])

    def elements(self) -> dict[str, dict]:
        result = self.call("get_ui_tree")

        return result.get("structuredContent", {}).get("nodes", {})

    def image(self, tool: str, arguments: dict | None = None) -> bytes:
        """Calls a screenshot tool, retrying while the slow renderer has not presented a frame."""

        for attempt in range(1, SCREENSHOT_ATTEMPTS + 1):
            try:
                result = self.call(tool, arguments)
            except ToolError as error:
                if not error.is_timeout or attempt == SCREENSHOT_ATTEMPTS:
                    raise ScreenshotError(f"{tool} failed after {attempt} attempts: {error.message}") from error

                # gpui-mcp waits two seconds for the frame it requested. On a
                # busy machine that frame takes longer; let it finish first.
                time.sleep(SCREENSHOT_RETRY_SECONDS)
                continue

            for block in result.get("content", []):
                if block.get("type") == "image":
                    return base64.b64decode(block["data"])

            raise ScreenshotError(f"{tool} returned no image")

        raise AssertionError("unreachable")

    def close(self) -> None:
        if self.process.stdin:
            self.process.stdin.close()

        stop_process(self.process, grace_seconds=5)
        self.log_file.close()


# ---------------------------------------------------------------------------
# DBFlux process
# ---------------------------------------------------------------------------


@dataclass
class Environment:
    """Everything a DBFlux launch needs, fixed for the whole run."""

    dbflux_command: list[str]
    mcp_command: list[str]
    vulkan_icd: Path
    work_dir: Path
    display: str = ""

    def process_environment(self) -> dict[str, str]:
        environment = dict(os.environ)
        environment.update({f"XDG_{name.upper()}_HOME": str(self.work_dir / name) for name in PROFILE_DIRS})
        environment.update(
            {
                "DISPLAY": self.display,
                "WAYLAND_DISPLAY": "",
                "GPUI_X11_SCALE_FACTOR": str(UI_SCALE),
                "VK_ICD_FILENAMES": str(self.vulkan_icd),
                "RUST_LOG": "info",
                # English UI and UTC times, whatever the host uses.
                "LANG": "en_US.UTF-8",
                "LC_ALL": "en_US.UTF-8",
                "LANGUAGE": "en",
                "TZ": "UTC",
            }
        )

        return environment

    def placeholders(self) -> dict[str, str]:
        values = {f"{service.name}_port": str(service.host_port) for service in DEMO_SERVICES}
        values["sqlite_path"] = str(self.work_dir / "inventory.db")

        return values

    def save_profile(self, name: str) -> None:
        snapshot = self.work_dir / "profiles" / name
        shutil.rmtree(snapshot, ignore_errors=True)
        snapshot.mkdir(parents=True)

        for directory in PROFILE_DIRS:
            if (self.work_dir / directory).exists():
                shutil.copytree(self.work_dir / directory, snapshot / directory, symlinks=True)

    def restore_profile(self, name: str) -> None:
        snapshot = self.work_dir / "profiles" / name

        for directory in PROFILE_DIRS:
            shutil.rmtree(self.work_dir / directory, ignore_errors=True)

            if (snapshot / directory).exists():
                shutil.copytree(snapshot / directory, self.work_dir / directory, symlinks=True)


class DBFluxInstance:
    """One DBFlux process, with a gpui-mcp session for each window in use."""

    def __init__(self, environment: Environment, label: str):
        self.environment = environment
        self.log_path = environment.work_dir / "logs" / f"dbflux-{label}.log"
        self.process = self.launch()
        self.windows: list[McpSession] = []

        try:
            self.main_descriptor = self.wait_for_descriptor()
            self.windows.append(self.open_session(self.main_descriptor))
        except BaseException:
            self.stop()
            raise

    @property
    def main(self) -> McpSession:
        return self.windows[0]

    @property
    def current(self) -> McpSession:
        return self.windows[-1]

    def launch(self) -> subprocess.Popen:
        with self.log_path.open("w") as log_file:
            return subprocess.Popen(
                self.environment.dbflux_command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=self.environment.process_environment(),
                start_new_session=True,
            )

    def wait_for_descriptor(self) -> Path:
        deadline = time.monotonic() + DESCRIPTOR_TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            match = DESCRIPTOR_PATTERN.search(self.log_path.read_text(errors="replace"))

            if match:
                return Path(match.group(1))

            if self.process.poll() is not None:
                raise ScreenshotError(
                    f"DBFlux exited with status {self.process.returncode} before its automation bridge "
                    "started. A running DBFlux of the same build profile (release or debug) holds the "
                    "app-control socket and makes a second one exit. Last log lines:\n" + tail(self.log_path)
                )

            time.sleep(0.25)

        raise ScreenshotError(
            "DBFlux did not log its automation bridge descriptor. Was it built with "
            "`--features ui-automation`? Last log lines:\n" + tail(self.log_path)
        )

    def open_session(self, descriptor: Path) -> McpSession:
        return McpSession(
            self.environment.mcp_command,
            descriptor,
            self.environment.process_environment(),
            self.environment.work_dir / "logs" / "gpui-mcp.log",
        )

    def publish_windows(self) -> None:
        publish_client_list(self.environment.display, visible_windows(self.environment.display, self.process.pid))

    def window_descriptors(self) -> set[Path]:
        return set(self.main_descriptor.parent.glob(f"*-{self.process.pid}-*.json"))

    def open_window(self, keystroke: str) -> None:
        """Presses `keystroke`, waits for the window it opens and makes it current."""

        before = self.window_descriptors()

        # The key is pressed again only when no window opened, since a key
        # that arrives late would otherwise open a second one.
        for _attempt in range(KEY_ATTEMPTS):
            press_key(self.current, keystroke)

            if opened := self.wait_for_descriptors(lambda: self.window_descriptors() - before):
                self.windows.append(self.open_session(sorted(opened)[0]))
                self.publish_windows()
                return

        raise ScreenshotError(f"{keystroke} did not open a new DBFlux window")

    @staticmethod
    def wait_for_descriptors(probe) -> set[Path]:
        deadline = time.monotonic() + KEY_RETRY_SECONDS

        while not (found := probe()) and time.monotonic() < deadline:
            time.sleep(0.25)

        return found

    def close_window(self, keystroke: str) -> None:
        """Presses `keystroke` in the current secondary window and waits for it to close."""

        if len(self.windows) == 1:
            raise ScreenshotError("close_window needs a window opened with open_window")

        window = self.windows[-1]

        for _attempt in range(KEY_ATTEMPTS):
            try:
                press_key(window, keystroke)
            except ToolError as error:
                # The window can close before the bridge answers.
                if CLOSED_CONNECTION_ERROR not in error.message:
                    raise

            if self.wait_for_descriptors(lambda: set() if window.descriptor.exists() else {window.descriptor}):
                self.windows.pop()
                window.close()
                self.publish_windows()
                return

        raise ScreenshotError(f"{keystroke} did not close the window")

    def save_failure_screenshot(self) -> Path | None:
        """Saves what the current window shows, for a failed step. Never raises."""

        path = self.log_path.with_suffix(".failure.png")

        try:
            path.write_bytes(self.current.image("screenshot"))
        except (ScreenshotError, ToolError, OSError, IndexError):
            return None

        return path

    def detach(self) -> None:
        """Closes the MCP sessions and leaves DBFlux running, for --keep-running."""

        for window in reversed(self.windows):
            window.close()

        self.windows.clear()
        log(
            f"DBFlux left running: pid {self.process.pid}, DISPLAY={self.environment.display}, "
            f"main window descriptor {self.main_descriptor}"
        )

    def stop(self) -> None:
        for window in reversed(self.windows):
            window.close()

        self.windows.clear()
        stop_process(self.process)


def press_key(session: McpSession, keystroke: str) -> None:
    try:
        session.call("keyboard", {"keystroke": keystroke})
    except ToolError as error:
        # The renderer may miss the frame deadline after delivering the key.
        if not error.is_timeout:
            raise


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

WAIT_TOOLS = {"wait_for_element", "wait_for_state", "wait_for_idle"}


PLACEHOLDER_PATTERN = re.compile(r"\{(\w+)\}")


def fill_placeholders(value, placeholders: dict[str, str]):
    # Only known names are replaced, so braces in a query or a JSON filter
    # pass through untouched.
    if isinstance(value, str):
        return PLACEHOLDER_PATTERN.sub(lambda match: placeholders.get(match.group(1), match.group(0)), value)

    if isinstance(value, dict):
        return {key: fill_placeholders(item, placeholders) for key, item in value.items()}

    if isinstance(value, list):
        return [fill_placeholders(item, placeholders) for item in value]

    return value


def find_labelled(session: McpSession, arguments: dict) -> dict:
    """The element labelled `label`, taking the `index`-th match."""

    label = arguments["label"]
    index = arguments.get("index", 0)
    matches = session.find(label, exact=arguments.get("exact", True))

    if len(matches) <= index:
        raise ScreenshotError(f"no element labelled {label!r} (match {index}) is visible")

    return matches[index]


def resolve_label(session: McpSession, arguments: dict) -> dict:
    """Replaces a `label` argument with the `id` of the element carrying that label.

    Many elements, such as sidebar rows and editors, have ids that change from
    run to run, while their labels do not.
    """

    if "label" not in arguments:
        return arguments

    resolved = {key: value for key, value in arguments.items() if key not in ("label", "exact", "index")}
    resolved["id"] = find_labelled(session, arguments)["id"]

    return resolved


def click_label(session: McpSession, arguments: dict) -> None:
    """Clicks the center of the element labelled `label`.

    A pointer click selects rows, such as sidebar tree rows, that do not
    expose a click action.
    """

    # One lookup for both the match and its bounds: the tree can change between
    # two lookups while a connection opens.
    bounds = find_labelled(session, arguments)["bounds"]
    center = {"x": bounds["x"] + bounds["width"] / 2, "y": bounds["y"] + bounds["height"] / 2}

    try:
        session.call("click_coordinates", center)
    except ToolError as error:
        if not error.is_timeout:
            raise


def wait_gone(session: McpSession, pattern: str, timeout_ms: int) -> None:
    """Waits until no element id matches `pattern`, such as a toast that closes itself."""

    expression = re.compile(pattern)
    deadline = time.monotonic() + timeout_ms / 1000

    while matching := sorted(element for element in session.elements() if expression.search(element)):
        if time.monotonic() > deadline:
            raise ScreenshotError(f"elements {', '.join(matching)} did not go away")

        time.sleep(0.5)


def wait_selected(session: McpSession, label: str, exact: bool, timeout_ms: int) -> None:
    """Waits until the element labelled `label`, such as a sidebar row, is selected."""

    deadline = time.monotonic() + timeout_ms / 1000

    while not any(element["state"].get("selected") for element in session.find(label, exact=exact)):
        if time.monotonic() > deadline:
            raise ScreenshotError(f"{label!r} is not selected")

        time.sleep(0.5)


def wait_value(session: McpSession, element_id: str, value: str, timeout_ms: int) -> None:
    """Waits until the value of the input `element_id` is `value`."""

    deadline = time.monotonic() + timeout_ms / 1000

    while True:
        current = (session.elements().get(element_id, {}).get("value") or {}).get("value")

        if current == value:
            return

        if time.monotonic() > deadline:
            raise ScreenshotError(f"{element_id} holds {current!r}, expected {value!r}")

        time.sleep(0.5)


def check_passes(instance: DBFluxInstance, check: shot_list.Step, timeout_ms: int, placeholders: dict) -> bool:
    arguments = {**fill_placeholders(check.arguments, placeholders), "timeout_ms": timeout_ms}

    try:
        run_tool(instance, check.tool, arguments)
    except (ToolError, ScreenshotError):
        return False

    return True


def ensure(instance: DBFluxInstance, arguments: dict, placeholders: dict) -> None:
    """Runs the `actions` until the `check` passes.

    Under the software renderer an input can be dropped, or delivered after
    its tool call already timed out. The check runs before the first attempt
    and once more after the window settles, so an action that landed late is
    not repeated: repeating a toggle would undo it.
    """

    check = arguments["check"]
    attempts = arguments["attempts"]

    if check_passes(instance, check, ENSURE_QUICK_CHECK_MS, placeholders):
        return

    for attempt in range(1, attempts + 1):
        try:
            for action in arguments["actions"]:
                run_step(instance, action, placeholders)
        except ScreenshotError as error:
            log(f"  {error}")

        if check_passes(instance, check, ENSURE_CHECK_MS, placeholders):
            return

        check_passes(instance, shot_list.idle(), ENSURE_CHECK_MS, placeholders)

        if check_passes(instance, check, ENSURE_QUICK_CHECK_MS, placeholders):
            return

        log(f"  {check.tool} {json.dumps(check.arguments)} not reached, attempt {attempt} of {attempts}")

    raise ScreenshotError(f"{check.tool} {json.dumps(check.arguments)} was not reached after {attempts} attempts")


def run_step(instance: DBFluxInstance, step: shot_list.Step, placeholders: dict[str, str]) -> None:
    arguments = fill_placeholders(step.arguments, placeholders)

    try:
        run_tool(instance, step.tool, arguments, placeholders)
    except ToolError as error:
        if step.optional:
            return

        # An input tool can report a missed frame deadline after the input was
        # delivered. The wait steps that follow it check the resulting state.
        if error.is_timeout and step.tool not in WAIT_TOOLS:
            log(f"  {step.tool}: {error.message}; continuing, the next wait checks the state")
            return

        raise ScreenshotError(f"step {step.tool} {json.dumps(arguments)} failed: {error.message}") from error
    except ScreenshotError:
        if not step.optional:
            raise


def run_tool(instance: DBFluxInstance, tool: str, arguments: dict, placeholders: dict | None = None) -> None:
    session = instance.current
    timeout_ms = arguments.get("timeout_ms", shot_list.WAIT_MS)

    if tool == "pause":
        time.sleep(arguments["seconds"])
    elif tool == "open_window":
        instance.open_window(arguments["keystroke"])
    elif tool == "close_window":
        instance.close_window(arguments["keystroke"])
    elif tool == "click_label":
        click_label(session, arguments)
    elif tool == "wait_gone":
        wait_gone(session, arguments["pattern"], timeout_ms)
    elif tool == "wait_selected":
        wait_selected(session, arguments["label"], arguments.get("exact", True), timeout_ms)
    elif tool == "wait_value":
        wait_value(session, arguments["id"], arguments["value"], timeout_ms)
    elif tool == "ensure":
        ensure(instance, arguments, placeholders or {})
    else:
        session.call(tool, resolve_label(session, arguments))


def run_steps(instance: DBFluxInstance, steps, placeholders: dict[str, str]) -> None:
    for step in steps:
        try:
            run_step(instance, step, placeholders)
        except ScreenshotError as error:
            screenshot = instance.save_failure_screenshot()

            if screenshot is None:
                raise

            raise ScreenshotError(f"{error}\nThe window at the time of the failure: {screenshot}") from error


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------


def png_size(data: bytes) -> tuple[int, int]:
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        raise ScreenshotError("the screenshot is not a PNG")

    return struct.unpack(">II", data[16:24])


def capture(instance: DBFluxInstance, shot: shot_list.Shot) -> bytes:
    session = instance.current
    run_steps(instance, shot_list.SETTLE_STEPS, {})

    if shot.region is not None:
        return session.image("screenshot_region", shot.region.as_arguments())

    data = session.image("screenshot")
    size = png_size(data)
    expected = (WINDOW_WIDTH * UI_SCALE, WINDOW_HEIGHT * UI_SCALE)

    if session is instance.main and size != expected:
        raise ScreenshotError(f"the main window captured as {size[0]}x{size[1]}, expected {expected[0]}x{expected[1]}")

    return data


def write_webp(png: bytes, crop: shot_list.Region | None, destination: Path, scratch: Path) -> None:
    """Crops (in logical pixels), scales to one pixel per logical pixel and encodes WebP."""

    width, height = png_size(png)

    if crop is None:
        crop = shot_list.Region(0, 0, width / UI_SCALE, height / UI_SCALE)

    source = scratch / "capture.png"
    source.write_bytes(png)
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".partial")

    command = ["cwebp", "-quiet", "-q", str(WEBP_QUALITY), "-sharp_yuv", "-metadata", "none"]
    command += ["-crop", *(str(round(value * UI_SCALE)) for value in crop.as_tuple())]
    command += ["-resize", str(round(crop.width)), str(round(crop.height))]
    command += [str(source), "-o", str(partial)]

    result = run_command(command, check=False)

    if result.returncode != 0:
        raise ScreenshotError(f"cwebp failed for {destination}: {result.stderr.strip()}")

    partial.replace(destination)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def start_instance(environment: Environment, label: str) -> DBFluxInstance:
    instance = DBFluxInstance(environment, label)

    try:
        run_steps(instance, shot_list.STARTUP_STEPS, {})
        place_main_window(environment.display, instance.process.pid)
        instance.publish_windows()
        wait_for_window_layout(instance.main)
        run_steps(instance, shot_list.SETTLE_STEPS, {})
    except BaseException:
        instance.stop()
        raise

    return instance


def prepare_profiles(environment: Environment, themes: tuple[str, ...]) -> None:
    """Creates the demo connections, then saves one profile per theme."""

    for index, theme in enumerate(themes):
        if index > 0:
            environment.restore_profile(themes[0])

        instance = start_instance(environment, f"setup-{theme}")

        try:
            if index == 0:
                log("creating the demo connections")
                run_steps(instance, shot_list.FIRST_RUN_STEPS, environment.placeholders())

            log(f"selecting the {theme} theme")
            run_steps(instance, shot_list.theme_steps(theme), environment.placeholders())
        finally:
            instance.stop()

        environment.save_profile(theme)


def take_shot(
    environment: Environment,
    shot: shot_list.Shot,
    theme: str,
    out_dir: Path,
    *,
    keep_running: bool,
    last: bool,
) -> Path:
    """Takes one shot in a fresh DBFlux process.

    With `keep_running` the process of a failed shot is left running, and so
    is the process of a successful one when `last` is set, so the screen the
    steps reached can be inspected.
    """

    environment.restore_profile(theme)
    instance = start_instance(environment, f"{shot.page}-{shot.name}-{theme}")
    succeeded = False

    try:
        run_steps(instance, shot.steps, environment.placeholders())
        png = capture(instance, shot)
        succeeded = True
    finally:
        if keep_running and (last or not succeeded):
            instance.detach()
        else:
            instance.stop()

    destination = out_dir / shot.page / f"{shot.name}-{theme}.webp"
    write_webp(png, shot.crop, destination, environment.work_dir)

    return destination


def run(arguments: argparse.Namespace) -> int:
    selected = select_shots(arguments.only)
    themes = THEMES if arguments.theme == "both" else (arguments.theme,)
    out_dir = arguments.out.resolve()
    environment = check_prerequisites(arguments)

    with ExitStack() as cleanup:
        if not arguments.keep_running:
            cleanup.callback(remove_demo_containers)

        reset_work_dir(environment.work_dir)

        log("starting the demo databases")
        start_demo_databases()
        create_sqlite_database(environment.work_dir / "inventory.db")

        xvfb, environment.display = start_xvfb(environment.work_dir / "logs" / "xvfb.log")
        log(f"Xvfb is running on {environment.display}")

        if not arguments.keep_running:
            cleanup.callback(stop_process, xvfb)

        prepare_profiles(environment, themes)
        runs = [(theme, shot) for theme in themes for shot in selected]
        produced = []

        for index, (theme, shot) in enumerate(runs):
            log(f"{shot.page}/{shot.name} ({theme})")
            produced.append(
                take_shot(
                    environment,
                    shot,
                    theme,
                    out_dir,
                    keep_running=arguments.keep_running,
                    last=index == len(runs) - 1,
                )
            )

        for path in produced:
            log(f"wrote {path} ({path.stat().st_size // 1024} KiB)")

        if arguments.keep_running:
            log(
                f"left running: containers {CONTAINER_PREFIX}*, Xvfb on {environment.display} "
                f"(pid {xvfb.pid}), work directory {environment.work_dir}"
            )

    return 0


def select_shots(filters: list[str] | None) -> list[shot_list.Shot]:
    if not filters:
        return list(shot_list.SHOTS)

    known = {f"{shot.page}/{shot.name}": shot for shot in shot_list.SHOTS}
    unknown = [name for name in filters if name not in known]

    if unknown:
        raise ScreenshotError(f"unknown shot {', '.join(unknown)}; known shots: {', '.join(sorted(known))}")

    return [known[name] for name in filters]


def check_prerequisites(arguments: argparse.Namespace) -> Environment:
    problems = []

    for tool, hint in (
        ("docker", "install Docker and start its daemon"),
        ("Xvfb", "run inside `nix develop`, or install the Xvfb X server"),
        ("xdotool", "run inside `nix develop`, or install xdotool"),
        ("cwebp", "run inside `nix develop`, or install the cwebp tool of libwebp"),
    ):
        if shutil.which(tool) is None:
            problems.append(f"`{tool}` was not found on PATH: {hint}")

    if shutil.which("docker") and run_command(["docker", "info"], check=False).returncode != 0:
        problems.append("the Docker daemon is not reachable (`docker info` failed)")

    vulkan_icd = arguments.vulkan_icd or os.environ.get("DBFLUX_DOCS_VULKAN_ICD")

    if not vulkan_icd:
        problems.append(
            "no software Vulkan driver: run inside `nix develop`, which sets DBFLUX_DOCS_VULKAN_ICD, "
            "or pass --vulkan-icd <mesa>/share/vulkan/icd.d/lvp_icd.x86_64.json"
        )
    elif not Path(vulkan_icd).is_file():
        problems.append(f"the Vulkan ICD file {vulkan_icd} does not exist")

    for label, binary, build in (
        ("DBFlux", arguments.dbflux, "cargo build --release -p dbflux --features ui-automation"),
        ("gpui-mcp", arguments.gpui_mcp, "cargo build -p gpui-mcp-server"),
    ):
        if not binary.is_file():
            problems.append(f"the {label} binary {binary} does not exist: build it with `{build}`")

    if problems:
        raise ScreenshotError("cannot start:\n  - " + "\n  - ".join(problems))

    launcher = shlex.split(arguments.launcher) if arguments.launcher else []

    return Environment(
        dbflux_command=[*launcher, str(arguments.dbflux.resolve())],
        mcp_command=[*launcher, str(arguments.gpui_mcp.resolve())],
        vulkan_icd=Path(vulkan_icd).resolve(),
        work_dir=arguments.work_dir.absolute(),
    )


def reset_work_dir(work_dir: Path) -> None:
    """Empties the work directory, refusing to delete one this script did not create.

    The directory is wiped on every run, so a mistyped `--work-dir` must not
    point it at someone's files: only a directory carrying the marker file, or
    an empty one, is reused.
    """

    if work_dir.is_symlink():
        raise ScreenshotError(f"the work directory {work_dir} is a symbolic link; refusing to use it")

    if work_dir.exists():
        if not work_dir.is_dir():
            raise ScreenshotError(f"the work directory {work_dir} is not a directory")

        if any(work_dir.iterdir()) and not (work_dir / WORK_DIR_MARKER).is_file():
            raise ScreenshotError(
                f"the work directory {work_dir} is not empty and was not created by this script; "
                "pass an empty or new --work-dir"
            )

        try:
            shutil.rmtree(work_dir)
        except OSError as error:
            raise ScreenshotError(f"could not clear the work directory {work_dir}: {error}") from error

    try:
        work_dir.mkdir(parents=True, mode=0o700)
    except OSError as error:
        raise ScreenshotError(f"could not create the work directory {work_dir}: {error}") from error

    (work_dir / WORK_DIR_MARKER).write_text("Created by scripts/docs_screenshots.py; deleted on every run.\n")
    (work_dir / "logs").mkdir()


def stop_process(process: subprocess.Popen, grace_seconds: float = 20) -> None:
    if process.poll() is not None:
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        process.terminate()

    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            process.kill()

        process.wait()


def tail(path: Path, lines: int = 15) -> str:
    try:
        return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])
    except OSError:
        return "(no log)"


def log(message: str) -> None:
    print(f"[docs-screenshots] {message}", file=sys.stderr, flush=True)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Regenerate the DBFlux documentation screenshots.")
    parser.add_argument(
        "--out", type=Path, default=REPO_ROOT / "docs" / "images", help="output directory (default: docs/images)"
    )
    parser.add_argument(
        "--only", action="append", metavar="PAGE/NAME", help="take only this shot; repeat the option for several"
    )
    parser.add_argument("--theme", choices=("light", "dark", "both"), default="both")
    parser.add_argument(
        "--keep-running",
        action="store_true",
        help="leave the containers, Xvfb and a failed shot's DBFlux running for inspection",
    )
    parser.add_argument(
        "--dbflux",
        type=Path,
        default=REPO_ROOT / "target" / "release" / "dbflux",
        help="DBFlux binary built with --features ui-automation (default: target/release/dbflux)",
    )
    parser.add_argument(
        "--gpui-mcp",
        type=Path,
        default=REPO_ROOT / "target" / "debug" / "gpui-mcp",
        help="gpui-mcp server binary (default: target/debug/gpui-mcp)",
    )
    parser.add_argument(
        "--launcher",
        help="command that prefixes both binaries, such as a dynamic loader with its --library-path",
    )
    parser.add_argument(
        "--vulkan-icd", help="ICD file of a software Vulkan driver (default: $DBFLUX_DOCS_VULKAN_ICD)"
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=DEFAULT_WORK_DIR,
        help=f"scratch directory for the DBFlux profile and logs, wiped on start (default: {DEFAULT_WORK_DIR})",
    )

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    arguments = parse_arguments(argv)

    try:
        return run(arguments)
    except ScreenshotError as error:
        log(f"error: {error}")
        return 1
    except KeyboardInterrupt:
        log("interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
