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
- **Value types**: `InstanceMetric`, `InstanceInspector`,
  `DefaultInstanceDashboard`, `InspectorRowAction`

Drivers expose live server metrics (e.g. `pg.tps`,
`mysql.queries_per_sec`) and tabular inspectors (e.g. `pg.activity`,
`mysql.processlist`, `mongo.currentop`, `redis.client_list`) through a
single catalog. Each driver also publishes a
`DefaultInstanceDashboard` descriptor with a fixed 12-column layout —
the workspace opens this descriptor as a **read-only Instance
Overview** dashboard (dedup key
`DocumentKey::InstanceOverview { profile_id }`). The "Save as
editable" action clones the layout into a persisted dashboard owned
by the user.

Inspector rows can declare `InspectorRowAction`s (e.g. *Terminate
connection*). Action availability is gated by per-driver privilege
probes (`pg_monitor` / `pg_signal_backend` for PostgreSQL, `PROCESS` /
`CONNECTION_ADMIN` for MySQL, `killOp` for MongoDB, `CLIENT KILL` for
Redis, `VIEW SERVER STATE` / `KILL` for SQL Server) so an
under-privileged session never sees actions it could not execute.

Every refresh timer (dashboard tick, chart standalone tick, inspector
tick) checks `AppState::connections()` for the panel's profile and
skips its work when the connection is closed; the timer stays alive
so refresh resumes automatically on reconnect.

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
