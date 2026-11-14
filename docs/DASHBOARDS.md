# Dashboards & Saved Charts

DBFlux persists chart configurations as **Saved Charts** and groups them into
**Dashboards** — a grid of chart panels (and optional markdown dividers) that
share a time range and refresh policy.

For chart engine internals (rendering, axes, decimation, palette), see
[`CHARTS.md`](./CHARTS.md). For the SQLite storage layer, see
[`ARCHITECTURE.md`](../ARCHITECTURE.md#storage--configuration).

## Overview

- A **SavedChart** is the persisted form of a chart configuration: data source
  binding, series, Y-axis bindings, refresh policy, and time-range preset.
- A **Dashboard** is a named grid of panels. Each panel is either a
  `Chart` slot (references a `SavedChart` by id) or a `Divider` slot
  (inline markdown header strip — no chart, no toolbar).
- Dashboards own a shared **time range** and **refresh policy** that propagate
  to every loaded chart panel via subscriptions.
- Remote dashboards (e.g. CloudWatch) can be **browsed** in the sidebar and
  **imported** into a local Dashboard when the driver advertises the right
  capabilities.

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
- Reopen a saved chart with **Open chart…** in the command palette — it lists the
  saved charts for the active connection. (If there are none: *"No saved charts
  for the current profile"*.)
- Saved charts also appear in the sidebar under **Saved Charts**, where each chart
  has **Open / Rename… / Duplicate / Delete**.

Charts are saved per connection profile.

## Dashboards

A dashboard is a named 12-column grid of panels that share one time range and one
refresh policy.

### Create one

1. Run **New dashboard…** from the command palette (or **New dashboard…** on the
   sidebar's **Dashboards** folder).
2. Name it. It opens with a 12-column grid and refresh turned off.

New dashboards open in **View** mode. The sidebar's Dashboards folder lists saved
dashboards with **Open / Rename… / Duplicate / Delete**.

### Edit vs. view

Switch between **View** and **Edit** in the header:

- **Edit** mode shows drag handles — drag panels to reorder, drag the edges or
  corner to resize within the 12-column grid.
- **View** mode is read-only.

The whole dashboard works from the keyboard in both modes: `h`/`j`/`k`/`l` or
the arrows select a panel, `Enter` opens it (its chart or table takes the keys
until `Escape`), `c` opens its Configure popover, `r` or `F2` renames it, `x` or
`Delete` removes it, `a` adds a panel and `m` lists every action. In Edit mode
`Shift` + `h`/`j`/`k`/`l` moves the selected panel and `Alt+Shift` +
`h`/`j`/`k`/`l` resizes it. See [Keyboard Reference](KEYBOARD.md#dashboards).

### Add panels

Click **+ Add Panel**. The picker has up to three tabs:

| Tab | Creates |
|-----|---------|
| **Saved** | A panel from one or more existing saved charts. |
| **Query** | A new panel from a name + a query you type. |
| **Metric** | A panel from a driver metric (only shown when the connection's driver exposes a metric catalog). |

Each panel's settings button (Edit mode) offers **Configure / Edit title /
Remove panel**. The **Configure** popover lets you change the chart type (Line, Bar,
Scatter, Area, Stacked, Pie), adjust axis bindings, and view **Stats**.

Dashboards can also contain **Divider** strips — markdown headers that visually
group panels and collapse the panels beneath them when clicked.

> A **Chart** panel references a saved chart. If you delete that saved chart, the
> panel becomes a placeholder ("Chart not found — saved chart was deleted") rather
> than disappearing.

### Time range and refresh

The header has:

- The **time-range** presets: 15m, 1h, 6h, 24h, 7d, or **Custom** (which reveals
  a row of date and hour/minute pickers). The range applies to every chart panel
  at once.
- A **refresh** split-button: click to refresh all panels now; the dropdown sets
  an auto-refresh interval (or Off / refresh-on-open).

> **Disconnected connections are handled gracefully.** When a panel's connection
> is closed, its refresh tick is skipped — the timer stays alive and resumes
> automatically when you reconnect, with no need to re-open the dashboard.

## Remote dashboards (CloudWatch)

When a driver supports it (CloudWatch is the reference implementation), DBFlux can
**browse** and **import** upstream dashboards.

- **Browse**: upstream dashboards appear in the sidebar. Opening one fetches and
  renders it as an **in-memory, read-only** dashboard — nothing is written back to
  the source, and nothing is saved locally. A **Refresh** action re-fetches the
  listing. The listing is session-scoped and is **not** kept across restarts.
- **Import**: the **Import dashboard from JSON…** command (palette or sidebar) parses the
  upstream definition into a new **local** dashboard with imported charts. It's
  only available when the active connection's driver supports import — otherwise
  you'll see *"The active connection does not support dashboard import."*

Remote dashboards are read-only browsing/import; DBFlux never modifies the source.

## Storage Layer

All dashboard and saved-chart data lives in `~/.local/share/dbflux/dbflux.db`
under the `viz_*` table prefix:

| Table | Purpose |
|-------|---------|
| `viz_dashboards` | Dashboard records (`profile_id` nullable, `ON DELETE SET NULL`) |
| `viz_dashboard_panels` | Panel slots: `panel_kind` discriminator + optional `divider_markdown` |
| `viz_saved_charts` | Saved chart root (`SavedChartDto`) |
| `viz_saved_chart_series` | Per-series settings |
| `viz_saved_chart_binding_y` | Y-axis bindings |
| `viz_saved_chart_source_metric_dimensions` | CloudWatch metric dimensions |
| `viz_saved_chart_source_metric_series` | CloudWatch metric series spec |

Repositories live in `crates/dbflux_storage/src/repositories/viz_*.rs` and
implement the standard `Repository` trait
(`all()`, `find_by_id()`, `upsert()`, `delete()`).

## In-Memory Managers

Both managers wrap the SQLite repositories with in-memory caches for
synchronous reads. Writes go to the repository first; caches update only on
success.

- **`DashboardManager`** (`crates/dbflux_ui_base/src/dashboard_manager.rs`) —
  domain types `Dashboard`, `DashboardPanel`, `DashboardPanelKind`
  (`Chart { saved_chart_id }` | `Divider { markdown }` |
  `Inspector { metric_id }`), `DashboardPanelDraft`. New dashboards are
  created with `grid_columns = 12`; new panels are appended at
  `grid_column = 0` on a new row with `grid_width = 12, grid_height = 2`.
- **`SavedChartManager`** (`crates/dbflux_ui_base/src/saved_chart_manager.rs`)
  — owns the `SavedChart` lifecycle, including `SavedChartRefreshPolicy`
  (`Off` / `Interval { every_secs }`).
- **`RemoteDashboardCache`** (`crates/dbflux_app/src/remote_dashboard_cache.rs`)
  — session-scoped in-memory cache for upstream dashboard listings.
  Not persisted across restart.

## Document System Integration

Dashboards open as a `DashboardDocument`
(`crates/dbflux_ui_document/src/dashboard/`):

- **Dedup key**: `DocumentKey::Dashboard { dashboard_id }` (persisted) or
  `DocumentKey::InstanceOverview { profile_id }` (auto-generated read-only).
- **Chart panels**: each slot wraps a `ChartDocument` entity
  (`Loaded`) or a placeholder for a deleted chart (`Orphan`).
- **Inspector panels**: each slot wraps an `InspectorPanel` entity that
  hosts a `DataGridPanel` and refreshes on the dashboard's shared
  interval. Driver-supplied row actions (e.g. terminate connection,
  cancel query) appear in the row context menu.
- **Shared toolbar**: a single `TimeRangePanel` propagates window changes
  to all loaded panels via subscriptions.
- **Concurrency**: panel re-execution is bounded by `PANEL_REEXEC_CAP` to
  avoid overwhelming the connection with concurrent queries.
- **Grid**: 12-column canonical grid; drag-to-reorder and drag-to-resize via
  `DragReorderState` / `DragResizeState` in `dashboard/builder.rs`.

Standalone saved charts open as `ChartDocument`
(`crates/dbflux_ui_document/src/chart_document/`), keyed by
`DocumentKey::Chart { saved_chart_id }`. `ChartDocument` renders standalone or
embedded inside a `DashboardDocument` panel.

## Instance Overview and inspectors

Connections whose driver advertises `INSTANCE_METRICS` or `INSTANCE_INSPECTOR`
expose a read-only **Instance Overview** — a synthesized dashboard of live
server metrics and tabular inspectors that never touches storage until the user
chooses to keep it.

For drivers that support it — **PostgreSQL, MySQL/MariaDB, MongoDB, Redis, SQL
Server, ClickHouse, and InfluxDB (v2)** — a connected profile shows an **Instance Overview** entry in the
sidebar, above the **Instance Metrics** and **Instance Inspectors** folders.

- **Instance Overview** is a read-only dashboard synthesized from the driver's
  default layout (one tab per connection). You can't edit it or add panels, but
  you can press **Save as editable** to clone its layout into a new, fully
  editable dashboard.
- **Instance Metrics** are time-series charts (e.g. connections, throughput).
- **Instance Inspectors** are tabular snapshots of live server state (e.g.
  Postgres `pg_stat_activity`, MySQL process list, MongoDB current operations,
  Redis client list), refreshed on the shared interval.

Live instance charts accumulate only what they observe during the current chart
session: a bounded in-memory buffer of the latest 120 samples, which is never
persisted or backfilled. The selected time window governs the displayed time
axis and filters the samples that exist — relative ranges roll forward on each
refresh, while a custom applied range stays fixed. An empty stretch in the
earlier part of the seven-day window means no sample was collected yet, not
that historical data exists, and **Refresh** fetches the current sample rather
than reconstructing the past; saving an editable overview saves the layout and
chart configuration, not the collected metric history.

### Opening the Instance Overview

The sidebar shows a single **Instance Overview** leaf under a connected profile
(above the *Instance Metrics* and *Instance Inspectors* folders). Clicking it —
or choosing **Open** from its right-click menu — opens the overview.

| Step | Detail |
|------|--------|
| Source | The driver's `DefaultInstanceDashboard` descriptor (fixed 12-column layout) returned by `InstanceCatalog::default_dashboard()` |
| Dedup | `DocumentKey::InstanceOverview { profile_id }` — one tab per connection; clicking again focuses the existing tab |
| Persistence | None. The `DashboardDocument` is built in memory at open time; no `viz_*` rows are written |
| Mode | Read-only. Edit-mode transitions, *Add Panel*, and the Edit/View toggle are suppressed; drag-reorder and drag-resize are disabled |

### Saving it as an editable dashboard

A read-only overview shows a **Save as editable** button (toolbar, right group;
tooltip *"Clone this overview into a new editable dashboard"*). It clones the
synthesized layout — including exact panel positions — into a new persisted,
user-owned `Dashboard` via `DashboardManager::append_panels` with explicit
`DraftGridLayout` per panel. The clone opens as a normal editable
`DocumentKey::Dashboard { dashboard_id }` tab. The original overview stays
read-only and re-synthesizes on each open.

### Inspector panels

Inspectors are the third dashboard panel kind, alongside `Chart` and `Divider`:

| Panel kind | Backing | Notes |
|------------|---------|-------|
| `Chart` | `SavedChart` reference (`saved_chart_id`) | Time-series chart |
| `Divider` | Inline markdown | Header strip; no toolbar |
| `Inspector` | `DashboardPanelKind::Inspector { metric_id }` | Tabular snapshot; refreshes on the shared interval |

`DashboardPanelKind::Inspector { metric_id }`
(`crates/dbflux_ui_base/src/dashboard_manager.rs`) carries no chart reference —
the inspector is identified by `metric_id` alone. Each inspector panel hosts a
`DataGridPanel` showing the current snapshot returned by
`InstanceCatalog::fetch_inspector_snapshot` (e.g. PostgreSQL `pg_stat_activity`,
MySQL `PROCESSLIST`, MongoDB `currentOp`, Redis `CLIENT LIST`).

Persistence: the `Inspector` value is stored in `viz_dashboard_panels.panel_kind`
with the inspector key in `inspector_metric_id`. Both were added by
**migration 014** (`014_viz_inspector_and_instance_metric`), which extends the
`panel_kind` CHECK — first introduced in migration 013 with `chart` / `divider`
— to also accept `inspector`. (Standalone inspector tabs opened directly from
the *Instance Inspectors* sidebar folder use
`DocumentKey::InstanceInspector { profile_id, metric_id }`.)

### Inspector row actions

Some inspectors offer per-row actions (for example **Kill connection** /
**Terminate session**). These are:

- **Permission-gated** — an action you don't have the privilege to run is hidden,
  so you never see a button that would just fail.
- **Confirmed when destructive** — destructive actions prompt before running.
- **Audited** — every attempt records an audit event, and failures surface a toast
  with a link to the matching audit row.

Inspector rows can expose driver-supplied row actions (right-click context
menu, or `m` on the selected row), e.g. *Kill connection* / *Terminate session*.
The flow:

1. The driver returns `InspectorRowAction`s from `InstanceCatalog::row_actions(metric_id)`. Availability is gated by per-driver privilege probes (see the driver READMEs), so an under-privileged session never sees an action it cannot run.
2. `is_destructive` actions prompt a confirmation modal before execution.
3. On confirm, the connection is re-resolved at execution time (not at click time) and `InstanceCatalog::execute_row_action(metric_id, action_id, row_values)` runs.
4. Every attempt records an audit event. Failures route through `report_error_async` (`UserFacingError` of `ErrorKind::Driver`), so the user gets a toast with a correlation id that links to the audit row.

Execution lives in
`crates/dbflux_ui_document/src/instance_inspector/mod.rs`.

### Refresh behavior

Dashboard, standalone-chart, and inspector refresh timers all check
`AppState::connections()` for the panel's profile before each tick and skip
their work when the connection is closed. The timer itself stays alive, so
refresh resumes automatically on reconnect without re-arming.

### Built-in driver coverage

| Driver | `INSTANCE_METRICS` | `INSTANCE_INSPECTOR` | Metric / inspector list |
|--------|:---:|:---:|---|
| PostgreSQL | ✓ | ✓ | [README](../crates/dbflux_driver_postgres/README.md) |
| MySQL / MariaDB | ✓ | ✓ | [README](../crates/dbflux_driver_mysql/README.md) |
| MongoDB | ✓ | ✓ | [README](../crates/dbflux_driver_mongodb/README.md) |
| Redis | ✓ | ✓ | [README](../crates/dbflux_driver_redis/README.md) |
| SQL Server | ✓ | ✓ | [README](../crates/dbflux_driver_mssql/README.md) |
| ClickHouse | ✓ | ✓ | [README](../crates/dbflux_driver_clickhouse/README.md) |
| InfluxDB (v2 only) | ✓ | ✓ | [README](../crates/dbflux_driver_influxdb/README.md) |

Each driver README lists the concrete metrics, inspectors, and row actions it
exposes; this document does not duplicate them.

## Driver Seams

Drivers opt into dashboard interop through generic core seams — the UI never
branches on driver IDs.

### Importing dashboards (JSON → local Dashboard)

- **Trait**: `DashboardImporter`
  (`crates/dbflux_core/src/connection/dashboard_import.rs`)
- **Capability**: `DriverCapabilities::DASHBOARD_IMPORT`
- **Value types**:
  - `WidgetImportSpec` — parsed widget spec
  - `MetricView::{TimeSeries, StackedArea, SingleValue}`
  - `ImportedMetricSeries` — series + dimensions
  - `WidgetLayout` — native layout coordinates carried through to the local
    grid

Drivers parse dashboard JSON into a normalized set of widgets that the UI
imports as `SavedChart`s and lays out on a new `Dashboard`.

### Browsing remote dashboards (sidebar)

- **Trait**: `DashboardSource`
  (`crates/dbflux_core/src/connection/dashboard_source.rs`)
- **Capability**: `DriverCapabilities::DASHBOARD_SYNC`
- **Value types**: `RemoteDashboard`, `DashboardRef`
  (optional `last_modified: ISO8601`)

The sidebar lists upstream dashboards through this seam; results are cached in
`RemoteDashboardCache`. Selecting a remote dashboard triggers `DashboardImporter`
to materialize it locally.

### Instance metrics and inspectors

- **Trait**: `InstanceCatalog`
  (`crates/dbflux_core/src/connection/instance_catalog.rs`)
- **Capabilities**: `DriverCapabilities::INSTANCE_METRICS` (time-series),
  `DriverCapabilities::INSTANCE_INSPECTOR` (tabular snapshots)
- **Value types**: `InstanceMetricDef`, `InstanceInspectorDef`,
  `DefaultInstanceDashboard`, `InspectorRowAction`

Drivers expose live server metrics (e.g. `pg.tps`,
`mysql.queries_per_sec`, `clickhouse.query`) and tabular inspectors
(e.g. `pg.activity`, `mysql.processlist`, `mongo.currentop`,
`redis.client_list`, `clickhouse.processes`) through a single catalog.
Each driver also publishes a `DefaultInstanceDashboard` descriptor with
a fixed 12-column layout — the workspace opens this descriptor as a
**read-only Instance Overview** dashboard (dedup key
`DocumentKey::InstanceOverview { profile_id }`). The "Save as
editable" action clones the layout into a persisted dashboard owned
by the user.

Inspector rows can declare `InspectorRowAction`s (e.g. *Terminate
connection*). Action availability is gated by per-driver privilege
probes (`pg_monitor` / `pg_signal_backend` for PostgreSQL, `PROCESS` /
`CONNECTION_ADMIN` for MySQL, `killOp` for MongoDB, `CLIENT KILL` for
Redis, `VIEW SERVER STATE` / `KILL` for SQL Server, `KILL QUERY` for
ClickHouse) so an under-privileged session never sees actions it could
not execute.

Every refresh timer (dashboard tick, chart standalone tick, inspector
tick) checks `AppState::connections()` for the panel's profile and
skips its work when the connection is closed; the timer stays alive
so refresh resumes automatically on reconnect.

For the user-facing behavior of this seam (how the Instance Overview
opens, *Save as editable*, inspector panels and row actions), see
[Instance Overview and inspectors](#instance-overview-and-inspectors)
above.

### CloudWatch implementation

`crates/dbflux_driver_cloudwatch/src/` provides:

- `CloudWatchDashboardSource` — lists CloudWatch dashboards via the AWS SDK
- `CloudWatchDashboardImporter` — parses CloudWatch dashboard JSON into
  `WidgetImportSpec`s with metric series, dimensions, and stat aggregations

This is **read-only browsing and import**, not a sync feature. DBFlux never
writes back to CloudWatch dashboards.

## Capability Matrix

| Capability bit | Meaning |
|---|---|
| `DASHBOARD_IMPORT` (51) | Driver can parse dashboard JSON into widget specs |
| `DASHBOARD_SYNC` (52) | Driver can list upstream dashboards |

Both capabilities are independent: a driver can advertise sync without import
or vice versa.

## Adding a New Dashboard-Capable Driver

1. Implement `DashboardSource` on the driver `Connection` to list upstream
   dashboards. Add `DASHBOARD_SYNC` to `DriverMetadata.capabilities`.
2. Implement `DashboardImporter` on the driver `Connection` to parse a
   dashboard payload into `WidgetImportSpec`s. Add `DASHBOARD_IMPORT`.
3. The UI will surface the dashboard tree in the sidebar and route imports
   without any driver-specific branching.
