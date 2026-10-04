"""Declarative shot list for `scripts/docs_screenshots.py`.

A shot is a page of the documentation, a name, and the steps that bring a
freshly started DBFlux into the state to capture. Each step is one call to a
tool of the UI automation MCP server (see docs/UI_AUTOMATION.md, "Tools"), or
one of the runner's own steps:

- `pause`: sleeps.
- `open_window` / `close_window`: press a key that opens or closes a
  secondary window (Connection Manager, Settings) and direct the following
  steps at it, or back at the main window.
- `click_label`: clicks the center of the element with a given label. Sidebar
  tree rows need it: their ids change from run to run and they expose no click
  action.
- `wait_gone`: waits until no element id matches a pattern.
- `wait_value`: waits until an input holds a given value.
- `ensure`: repeats a few actions until a check passes. Software rendering
  draws about one frame per second and an input is sometimes dropped, so
  every action whose effect matters is wrapped in one.

A tool step may name its element by `label` instead of `id`; the runner looks
the id up with `find_elements` right before the call.

Every shot starts from the same state: DBFlux just launched with the demo
connections saved, nothing connected and no tab open. A shot is captured in
the light and the dark theme, and written to
`docs/images/<page>/<name>-<theme>.webp`.

Strings in step arguments may use `{postgres_port}`, `{mongodb_port}`,
`{redis_port}` and `{sqlite_path}`, which the runner fills in.

Coordinates and regions are logical pixels of the 1600x900 main window. The
output image has one pixel per logical pixel.
"""

from __future__ import annotations

from dataclasses import dataclass, field

WAIT_MS = 30_000


@dataclass(frozen=True)
class Step:
    tool: str
    arguments: dict = field(default_factory=dict)
    # An optional step may fail without failing the shot.
    optional: bool = False


@dataclass(frozen=True)
class Region:
    x: float
    y: float
    width: float
    height: float

    def as_arguments(self) -> dict:
        return {"x": self.x, "y": self.y, "width": self.width, "height": self.height}

    def as_tuple(self) -> tuple[float, float, float, float]:
        return (self.x, self.y, self.width, self.height)


@dataclass(frozen=True)
class Shot:
    page: str
    name: str
    steps: tuple[Step, ...]
    # Capture only this part of the window with `screenshot_region`.
    region: Region | None = None
    # Crop the captured image afterwards, relative to the captured area.
    crop: Region | None = None


# -- Step helpers -------------------------------------------------------------


def call(tool: str, *, optional: bool = False, **arguments) -> Step:
    return Step(tool, arguments, optional)


def key(keystroke: str) -> Step:
    return call("keyboard", keystroke=keystroke)


def click(element_id: str) -> Step:
    return call("click_element", id=element_id)


def click_label(label: str, *, exact: bool = True) -> Step:
    return Step("click_label", {"label": label, "exact": exact})


def set_text(element_id: str, text: str) -> Step:
    return call("set_text", id=element_id, text=text)


def wait_checked(element_id: str, checked: bool) -> Step:
    return call("wait_for_state", id=element_id, checked=checked, timeout_ms=WAIT_MS)


def wait_gone(pattern: str) -> Step:
    """Waits until no element id matches the regular expression `pattern`."""

    return Step("wait_gone", {"pattern": pattern})


def wait_for(label: str, *, exact: bool = False) -> Step:
    """Waits until an element whose label contains `label` is in the tree."""

    return call("wait_for_element", query=label, exact=exact, timeout_ms=WAIT_MS)


def wait_visible(element_id: str) -> Step:
    return call("wait_for_state", id=element_id, visible=True, timeout_ms=WAIT_MS)


def idle() -> Step:
    return call("wait_for_idle", timeout_ms=WAIT_MS)


def pause(seconds: float) -> Step:
    return Step("pause", {"seconds": seconds})


def open_window(keystroke: str) -> Step:
    """Presses `keystroke` and directs the following steps at the window it opens."""

    return Step("open_window", {"keystroke": keystroke})


def close_window(keystroke: str) -> Step:
    """Presses `keystroke` in the current window and returns to the main window."""

    return Step("close_window", {"keystroke": keystroke})


def ensure(check: Step, *actions: Step, attempts: int = 4) -> Step:
    """Runs `actions` until `check` passes, at most `attempts` times.

    Under the software renderer an input is sometimes dropped. The runner
    checks before the first attempt and again after the window settles, so an
    action that took effect late is not repeated.
    """

    return Step("ensure", {"check": check, "actions": actions, "attempts": attempts})


def wait_value(element_id: str, value: str) -> Step:
    """Waits until the input `element_id` holds `value`."""

    return Step("wait_value", {"id": element_id, "value": value})


def wait_selected(label: str, *, exact: bool = True) -> Step:
    """Waits until the element labelled `label`, such as a sidebar row, is selected."""

    return Step("wait_selected", {"label": label, "exact": exact})


def open_sidebar_item(label: str, loaded: str, *, exact: bool = False) -> tuple[Step, ...]:
    """Opens the sidebar row `label`, connecting it if it is a connection.

    The row is selected with a click and opened with Enter, which is more
    reliable than a double click under the software renderer. Enter waits for
    the selection: pressed on another row it would open that one.
    """

    return (
        ensure(wait_selected(label), click_label(label)),
        ensure(wait_for(loaded, exact=exact), key("enter")),
    )


def create_connection(driver: str, fields: dict[str, str]) -> tuple[Step, ...]:
    """Creates a connection through the Connection Manager and saves it."""

    steps = [
        open_window("ctrl-shift-n"),
        ensure(wait_visible("cm-field-name"), click(f"cm-driver-card-{driver}")),
    ]
    steps += [
        ensure(wait_value(f"cm-field-{field_id}", value), set_text(f"cm-field-{field_id}", value))
        for field_id, value in fields.items()
    ]
    steps += [
        close_window("ctrl-s"),
        wait_for(fields["name"], exact=True),
    ]

    return tuple(steps)


# -- Runner hooks -------------------------------------------------------------

# Run after every launch, before the window is resized.
STARTUP_STEPS = (wait_visible("activity-rail"),)

# Run right before every capture. Success toasts close themselves after four
# seconds, and they carry the time of day.
SETTLE_STEPS = (
    wait_gone(r"^toast-\d"),
    idle(),
    pause(1.5),
)

POSTGRES = "Shop (PostgreSQL)"
MONGODB = "Reviews (MongoDB)"
REDIS = "Cache (Redis)"
SQLITE = "Inventory (SQLite)"

# Run once, in the first DBFlux launch of a run.
FIRST_RUN_STEPS = (
    # The first-run welcome dialog. Turning off the update check keeps the
    # shots from depending on the network or on a newer release.
    wait_visible("welcome-dialog"),
    ensure(wait_checked("welcome-check-for-updates", False), click("welcome-check-for-updates")),
    ensure(wait_checked("welcome-show-whats-new", False), click("welcome-show-whats-new")),
    ensure(wait_gone(r"^welcome-dialog$"), key("enter")),
    *create_connection(
        "postgres",
        {"name": POSTGRES, "host": "127.0.0.1", "port": "{postgres_port}", "database": "shop", "user": "postgres"},
    ),
    *create_connection(
        "mongodb",
        {"name": MONGODB, "host": "127.0.0.1", "port": "{mongodb_port}", "database": "shop"},
    ),
    *create_connection(
        "redis",
        {"name": REDIS, "host": "127.0.0.1", "port": "{redis_port}"},
    ),
    *create_connection(
        "sqlite",
        {"name": SQLITE, "path": "{sqlite_path}"},
    ),
)


def theme_steps(theme: str) -> tuple[Step, ...]:
    """Selects `theme` ("light" or "dark") in Settings > General and saves it."""

    return (
        open_window("ctrl-,"),
        wait_visible(f"segmented-theme-{theme}"),
        ensure(wait_checked(f"segmented-theme-{theme}", True), click(f"segmented-theme-{theme}")),
        # The footer marks unsaved changes until they are saved.
        ensure(wait_gone(r"^settings-unsaved-changes$"), click("save-general")),
        close_window("ctrl-w"),
    )


# -- Shots --------------------------------------------------------------------

# The document area right of the sidebar, below the title bar.
DOCUMENT_AREA = Region(360, 44, 1232, 818)

QUERY_RESULT_SQL = """SELECT c.country,
       count(*) AS orders,
       sum(o.total) AS revenue
FROM orders AS o
JOIN customers AS c ON c.id = o.customer_id
WHERE o.status IN ('delivered', 'shipped')
GROUP BY c.country
ORDER BY revenue DESC;"""

SHOTS = (
    Shot(
        page="usage",
        name="main-window",
        steps=(
            *open_sidebar_item(POSTGRES, "customers", exact=True),
            *open_sidebar_item("customers", "Ada Hayashi", exact=True),
        ),
    ),
    Shot(
        page="editor",
        name="query-result",
        steps=(
            *open_sidebar_item(POSTGRES, "customers", exact=True),
            ensure(wait_for("Enter SQL here"), key("ctrl-n")),
            ensure(
                wait_for("Canada", exact=True),
                call("focus_element", label="Enter SQL here", exact=False),
                call("set_text", label="Enter SQL here", exact=False, text=QUERY_RESULT_SQL),
                key("ctrl-enter"),
            ),
        ),
    ),
    Shot(
        page="key-value",
        name="sorted-set",
        steps=(
            *open_sidebar_item(REDIS, "db 0", exact=True),
            *open_sidebar_item("db 0", "leaderboard:"),
            ensure(wait_for("ZSET leaderboard:sales"), click_label("leaderboard:", exact=False)),
            ensure(wait_for("Ada Alvarez"), click_label("ZSET leaderboard:sales", exact=False)),
        ),
        crop=DOCUMENT_AREA,
    ),
)
