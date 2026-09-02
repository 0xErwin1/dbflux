# Dashboards & Audit — User Guide

How to chart and save queries, build dashboards, watch live instance metrics, and
use the audit viewer. This is the *how do I use it* companion to the internals
docs: [Charts](CHARTS.md), [Dashboards](DASHBOARDS.md), and [Audit](AUDIT.md).

---

## Saved charts

### Chart a query result

1. Run a query that returns tabular data.
2. Right-click in the result grid and choose **Chart this query**.

A chart document opens, seeded with your query, and runs automatically. The
option only appears when the result has a usable original query and DBFlux can
auto-detect chartable columns. Supported chart types: Line, Bar, Scatter, Area,
Stacked Bar, Pie. Axis detection uses each column's kind (time, numeric, text);
see [Charts](CHARTS.md) for the rules.

### Save and reopen

- In a chart document, press **Save** and give it a name. Re-saving the same chart
  overwrites it (no duplicate).
- Reopen a saved chart with **Open Chart…** in the command palette — it lists the
  saved charts for the active connection. (If there are none: *"No saved charts
  for the current profile"*.)
- Saved charts also appear in the sidebar under **Saved Charts**, where each chart
  has **Open / Rename… / Duplicate / Delete…**.

Charts are saved per connection profile.

---

## Dashboards

A dashboard is a named 12-column grid of panels that share one time range and one
refresh policy.

### Create one

1. Run **New Dashboard…** from the command palette (or **New Dashboard…** on the
   sidebar's **Dashboards** folder).
2. Name it. It opens with a 12-column grid and refresh turned off.

New dashboards open in **View** mode. The sidebar's Dashboards folder lists saved
dashboards with **Open / Rename… / Duplicate / Delete…**.

### Edit vs. view

Toggle the **pencil / eye** button in the toolbar:

- **Edit** mode shows drag handles — drag panels to reorder, drag the edges or
  corner to resize within the 12-column grid.
- **View** mode is read-only.

In Edit mode you can also use the keyboard on a focused panel: `F2` to rename,
`Delete`/`Backspace` to remove, `Enter` to open its Configure popover.

### Add panels

Click **+ Add Panel**. The picker has up to three tabs:

| Tab | Creates |
|-----|---------|
| **Saved** | A panel from one or more existing saved charts. |
| **Query** | A new panel from a name + a query you type. |
| **Metric** | A panel from a driver metric (only shown when the connection's driver exposes a metric catalog). |

Each panel's kebab menu (Edit mode) offers **Configure / Edit title / Remove
panel**. The **Configure** popover lets you change the chart type (Line, Bar,
Scatter, Area, Stacked, Pie), adjust axis bindings, view **Stats**, and **Export
PNG**.

Dashboards can also contain **Divider** strips — markdown headers that visually
group panels and collapse the panels beneath them when clicked.

> A **Chart** panel references a saved chart. If you delete that saved chart, the
> panel becomes a placeholder ("Chart not found — saved chart was deleted") rather
> than disappearing.

### Time range and refresh

The toolbar has:

- A **time-range** dropdown: Last 15 min, Last 1 hour, Last 6 hours, Last 24
  hours, Last 7 days, or **Custom** (which reveals date and hour/minute pickers).
  The range applies to every chart panel at once.
- A **refresh** split-button: click to refresh all panels now; the dropdown sets
  an auto-refresh interval (or Off / refresh-on-open).

> **Disconnected connections are handled gracefully.** When a panel's connection
> is closed, its refresh tick is skipped — the timer stays alive and resumes
> automatically when you reconnect, with no need to re-open the dashboard.

---

## Instance Overview, metrics, and inspectors

For drivers that support it — **PostgreSQL, MySQL/MariaDB, MongoDB, Redis, and SQL
Server** — a connected profile shows an **Instance Overview** entry in the
sidebar, above the **Instance Metrics** and **Instance Inspectors** folders.

- **Instance Overview** is a read-only dashboard synthesized from the driver's
  default layout (one tab per connection). You can't edit it or add panels, but
  you can press **Save as editable** to clone its layout into a new, fully
  editable dashboard.
- **Instance Metrics** are time-series charts (e.g. connections, throughput).
- **Instance Inspectors** are tabular snapshots of live server state (e.g.
  Postgres `pg_stat_activity`, MySQL process list, MongoDB current operations,
  Redis client list), refreshed on the shared interval.

### Inspector row actions

Some inspectors offer per-row actions (for example **Kill connection** /
**Terminate session**). These are:

- **Permission-gated** — an action you don't have the privilege to run is hidden,
  so you never see a button that would just fail.
- **Confirmed when destructive** — destructive actions prompt before running.
- **Audited** — every attempt records an audit event, and failures surface a toast
  with a link to the matching audit row.

---

## Remote dashboards (CloudWatch)

When a driver supports it (CloudWatch is the reference implementation), DBFlux can
**browse** and **import** upstream dashboards.

- **Browse**: upstream dashboards appear in the sidebar. Opening one fetches and
  renders it as an **in-memory, read-only** dashboard — nothing is written back to
  the source, and nothing is saved locally. A **Refresh** action re-fetches the
  listing. The listing is session-scoped and is **not** kept across restarts.
- **Import**: the **Import Dashboard** command (palette or sidebar) parses the
  upstream definition into a new **local** dashboard with imported charts. It's
  only available when the active connection's driver supports import — otherwise
  you'll see *"The active connection does not support dashboard import."*

Remote dashboards are read-only browsing/import; DBFlux never modifies the source.

---

## Audit viewer

The audit viewer is the single place to review everything DBFlux logged: queries,
connections, hooks, scripts, config changes, and AI/MCP governance decisions.

### Open it

- Keyboard: **Ctrl+Shift+A** (**Cmd+Shift+A** on macOS).
- Command palette: **Open Audit Viewer**.

There's one audit tab; reopening focuses the existing one.

### What you see

Each row shows a timestamp, a **severity** chip (ERROR/WARN/INFO), a **category**
chip, and a summary. Expand a row to see **Category, Outcome, Actor, Action,
Duration, and Summary**.

Categories at a glance:

| Category | Covers |
|----------|--------|
| **Query** | Query execution and scans. |
| **Connection** | Connect / disconnect / reconnect. |
| **Hook** | Connection-hook runs. |
| **Script** | Lua / Python / Bash script runs. |
| **Mcp** | AI client tool calls. |
| **Governance** | Policy decisions. |
| **Config** | Profile and settings changes. |
| **System** | Startup, migrations, and internal log events. |

### Filter

The toolbar offers free-text **search**, a **time range** (same presets as
dashboards, plus Custom), a **timestamp mode** (Local / UTC), and multi-select
filters for **Level** (Error/Warn/Info), **Category**, and **Outcome**
(Success/Failure/Cancelled). **Clear** resets them.

A row's context menu adds **Copy Row as CSV**, **Copy Summary**, and — when the
event has a correlation id — **Filter by Correlation**.

### Follow an error to its audit row

When something you did fails, DBFlux shows a toast with a **View in Audit** action.
Clicking it opens the audit viewer filtered to that exact event (matched by a
correlation id shared between the toast and the audit row). The **error badge** in
the status bar opens the viewer pre-filtered to recent user-facing failures.

### Export

The **Export** button writes the currently visible events to **CSV** or **JSON**
in your `~/Downloads` folder (`audit_export.csv` / `audit_export.json`), using the
extended schema (all fields, including structured details). A success toast
reports how many events were written and where.

### Retention

Old events can be purged on a retention schedule when configured (see
[Settings → Audit](SETTINGS.md#audit)). The audit log otherwise grows with use; it
lives in the same `dbflux.db` as everything else
([Data & Privacy](DATA_AND_PRIVACY.md#audit-and-privacy)).

---

## Related

- [Charts](CHARTS.md) — chart types, column kinds, axis auto-detection.
- [Dashboards](DASHBOARDS.md) — storage model and driver seams.
- [Audit](AUDIT.md) — full event schema and redaction.
- [Settings & Hooks](SETTINGS.md) — audit and refresh settings.
