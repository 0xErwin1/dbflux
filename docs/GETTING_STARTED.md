# Getting Started

This page takes you from a fresh install to your first query result. If DBFlux
is not installed yet, start with [Installing](INSTALL.md).

DBFlux is keyboard-first. Almost every action has both a mouse affordance and a
keyboard binding. The keybindings listed in these pages are the application
defaults; you can review and change the active keymap in **Settings →
Keybindings** (see [Settings](SETTINGS.md#keybindings)). Every default binding
is listed in the [Keyboard Reference](KEYBOARD.md).

## First launch

On startup DBFlux restores your previous session (open tabs). On a fresh
install there is nothing to restore, so focus defaults to the sidebar.

## Create a connection

Press `Ctrl+Shift+N` (`Cmd+Shift+N` on macOS) to open the Connection Manager,
pick a driver, fill in its form, and connect. The connection's schema then
appears in the sidebar. [Connecting](CONNECTIONS.md) covers the other ways to
open the Connection Manager, the driver picker, the Access tab (SSH, proxy,
managed access), and what happens when a connection fails.

## Run your first query

Open a new query tab with `Ctrl+n` (`Cmd+n` on macOS), type a query in the
language of the active connection, and press `Ctrl+Enter` (`Cmd+Enter`) to run
it. The result renders in a result tab inside the document.

## Next steps

- [Connecting](CONNECTIONS.md) — the Connection Manager, drivers, SSH tunnels,
  proxies, AWS SSO, and value sources.
- [Browsing the Schema](SCHEMA_BROWSER.md) — the sidebar, the schema tree,
  routines, and the schema diagram.
- [Running Queries](EDITOR.md) — query tabs, execution, scripts, the
  dangerous-query confirmation, and query history.
- [Visual Query Builder](QUERY_BUILDER.md) — composing SELECT, UPDATE, and
  DELETE without writing SQL.
- [Working with Results](RESULTS.md) — the data grid, record view, filtering,
  editing, and exporting.
- [Key-Value Browser](KEY_VALUE.md) — keys, values, expiry, and the command
  console.
- [Document Collections](DOCUMENTS.md) — table, tree, and JSON views of
  documents.
- [Charts](CHARTS.md) and [Dashboards](DASHBOARDS.md) — charting results and
  building dashboards.
- [Keyboard Reference](KEYBOARD.md) — every default binding, including Vim mode.
- [Settings](SETTINGS.md) — every Settings section and connection hooks.
