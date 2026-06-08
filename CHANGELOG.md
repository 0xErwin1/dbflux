# Changelog

All notable changes to DBFlux will be documented in this file.

## [Unreleased]

### Fixed

* **Chart auto-detection across four drivers (#204)** — Drivers now assign
  `ColumnKind` honestly so the chart engine includes genuine numeric columns
  and excludes non-plottable ones. CloudWatch CWL Insights `@timestamp` and
  `@ingestionTime` values are normalised from CWLI format (`YYYY-MM-DD
  HH:MM:SS.mmm`, UTC) to RFC3339 so the time axis can parse them; the kind
  scanner now skips `Text` samples and keeps scanning for a numeric value,
  so mixed-type columns resolve correctly. DynamoDB infers column kind from
  `AttributeValue` (`N` → `Integer` when the string parses as `i64`, else
  `Float`; `S` → `Text`; `Bool` → `Integer`; anything else → `Unknown`).
  MongoDB document and query results infer column kinds from BSON value types
  (Int32/Int64 → Integer, Double/Decimal128 → Float, Boolean → Integer,
  String → Text, DateTime → Timestamp); BSON `Timestamp` (oplog logical
  clock) stays `Unknown` because it carries no wall-clock meaning. InfluxDB
  Flux and InfluxQL kind mappers classify `boolean` as `Integer`. Across all
  drivers, `Value::Bool` now plots as 0/1, matching MSSQL BIT behaviour. The
  chart engine now extracts `Value::DateTime` and `Value::Date` as
  epoch-milliseconds on a time axis (Date as midnight UTC), so datetime/date
  columns from any driver can drive a time axis. `Value::Time` has no absolute
  epoch and remains unplottable, so SQL Server `TIME` columns are now
  classified `Unknown` instead of `Timestamp` (they would otherwise be offered
  as an empty time axis).

## [0.6.0] - 2026-06-04

### Added

* **Visual UPDATE / DELETE query builder (#163)** — The
  `QueryBuilderPanel` gains a mode selector that extends the visual SELECT
  builder with UPDATE and DELETE modes, reusing the relational filter bar
  for `WHERE` composition. The SQL preview is always visible and
  regenerates synchronously on every builder change. New core types
  (`VisualMutationSpec`, `MutationKind`, `ColumnAssignment`,
  `AssignmentValue`) feed `QueryGenerator::generate_update_from_spec` /
  `generate_delete_from_spec`, which emit keyset-paginated chunked DML for
  all four SQL dialects; a raw-expression assignment is tracked via a
  `used_raw_expression` flag rather than a textual marker. Execution runs
  through a `MutationExecutor` state machine with three modes —
  `SingleTransaction`, `ChunkedTransaction`, `DirectAutocommit` —
  auto-suggested from the count estimate, the `TRANSACTIONS` capability,
  and primary-key availability, with a tradeoff modal on user override.
  Chunked runs use keyset pagination over the table PK (chunk size
  clamped to `[1000, 10000]`, default 5000), surface per-chunk entries in
  the Tasks panel with cancellation between chunks, and `ROLLBACK` on
  chunk failure. No-`WHERE` UPDATE/DELETE is gated by a doubled
  spec-level + text-level `DangerousQueryKind` check, and a new
  `MutationPolicy` seam (`Allowed` / `ReadOnly` / `ApprovalRequired`)
  composes MCP-actor, per-profile read-only, and default resolution.
  Driver-agnostic by construction: gated on `QueryLanguage::Sql` with no
  per-driver branching.

* **Inline edit on builder-generated SELECT results (#170)** — Inline
  cell edit and row delete now work on results produced by the visual
  query builder, not just plain table browses, when the result is
  provably *editable-safe*: it maps 1:1 to a single underlying table
  and every primary-key column of that table is projected under its
  original name. The builder computes an `EditableBinding` from the
  committed `VisualQuerySpec` and threads it into the DataView, so edits
  and deletes reuse the existing single-table mutation path with a
  `WHERE` built from the projected PK values — no parsing of the
  generated SQL. JOINs are allowed: columns originating from the source
  table are editable while joined columns are marked read-only. The
  result falls back to read-only — with a toolbar hint explaining why —
  when any rule fails: aggregates / `GROUP BY` / `HAVING`, a wildcard
  projection across a JOIN, a primary key that is missing or projected
  under an alias, or a schema cache that has not yet loaded the table's
  keys (the grid upgrades to editable on its own once the keys arrive).
  Free-form editor SQL stays read-only; this is scoped to builder-
  generated queries. Driver-agnostic by construction: the editable-safe
  proof lives in `dbflux_core` over generic spec and metadata types with
  no per-driver branching, so every relational driver picks it up.

* **GROUP BY and aggregates in the visual query builder (#161)** — The
  visual SELECT builder gains a `Group By / Aggregates` section between
  Joins and Sort, with a separate `Having` section that reuses the same
  predicate editor as `Filters` (WHERE). Supported aggregate functions:
  `COUNT`, `COUNT(*)`, `COUNT(DISTINCT)`, `SUM`, `AVG`, `MIN`, `MAX`,
  each with an editable alias that auto-generates from the function and
  column. When the spec becomes grouped, the projection section is
  replaced by a read-only effective `SELECT` preview composed of group
  columns followed by aggregate aliases; sort entries are restricted to
  group columns and aggregate aliases, with invalid entries rejected
  with a visible error. The DataView reshapes in place: rows reflect
  the aggregated result, pagination switches to a `COUNT(*)` subquery
  over the grouped SELECT so the total page count is accurate, and
  aggregate result columns receive the correct `ColumnKind` so chart
  auto-detection keeps working (`COUNT*` → Integer, `AVG` → Float,
  `SUM` preserves Integer/Float, `MIN`/`MAX` preserve input). Editing
  is gated when the result is aggregated: add-row, delete-row,
  edit-cell, and inspect-row become unavailable with explanatory
  tooltips, and the footer surfaces a count of incomplete aggregate
  rows so silently-dropped rows are visible to the user. Driver-
  agnostic by construction: gated on `QueryLanguage::Sql` with no
  per-driver branching, and the existing `SqlSelectBuilder` is extended
  with `build_group_by`, `build_having`, and `build_count_of_grouped`
  shared across SQLite, PostgreSQL, MySQL/MariaDB, and SQL Server.

* **Schema-aware autocomplete for the visual query builder and DataView
  filter (#165)** — Inline suggestion popovers now appear on the
  builder rail's single-line inputs (filter / sort / projected columns,
  join target table, join `ON` left and right sides) and on the
  DataView toolbar's WHERE filter input. Suggestions are sourced from
  the live schema and the builder's own spec — source-table columns,
  declared join aliases (`alias.column`), and joined-table columns
  fetched lazily through the existing background metadata pattern.
  After typing `<alias>.`, results are scoped to that alias's columns
  only. Arrow keys navigate, `Tab` / `Enter` commits, `Esc` and focus
  loss dismiss. Prefix-only filtering for now (substring and SQL
  keyword completion are deliberately deferred). Driver-agnostic by
  construction: suggestions consume `dbflux_core` metadata types
  without branching on driver id, so every relational driver picks the
  feature up automatically.

* **Relational filters in the DataView filter bar (#162)** — The filter
  bar now accepts ORM-style dotted paths like
  `created_by.email LIKE '%@acme.com'` or
  `created_by.organization.name = 'Acme'`. Paths are resolved against
  foreign-key metadata cached on the data grid; the resolver lowers the
  expression into a `VisualQuerySpec` with `JoinOn::FkPath` joins and
  routes it through the same builder pipeline that ships with the
  visual SELECT builder (#146), so there is no second SQL generation
  path. Ambiguous segments surface an inline chip with an "Open in
  builder" action seeded with the joins resolved so far. The feature is
  driver-agnostic and gated on `QueryLanguage::Sql`; non-dotted input
  keeps today's raw-WHERE behavior.

* **Visual SELECT query builder (#146)** — A right-rail query builder
  composes SELECT statements without writing SQL: projection, FROM with
  alias, JOINs, a recursive `WHERE` predicate tree, `ORDER BY`, and
  `LIMIT`/`OFFSET`, with a live parameterized SQL preview. The foundation
  is the new `VisualQuerySpec` (and supporting `FilterNode`, `Predicate`,
  `JoinStep`, `JoinOn`, `Projection`, `SortEntry` types) in
  `dbflux_core`, rendered by `SqlSelectBuilder` behind the defaulted
  `QueryGenerator::generate_select` trait method with dialect-specific
  placeholders for SQLite, PostgreSQL, MySQL/MariaDB, and SQL Server.
  Builders can be saved and reopened: migration 017 adds `qry_saved_queries`
  and its child tables (columns, sorts, joins) with cascading FKs and a
  `UNIQUE (profile_id, name)` constraint, fronted by `SavedQueryRepo` and
  an in-memory `SavedQueryManager`. A `TableProbe` seam verifies table
  existence when importing a saved query onto another connection without
  reaching into driver code. A `column_kind` inference fallback maps
  `type_name` to `ColumnKind` so charts keep working on builder results.
  Driver-agnostic by construction: gated on `QueryLanguage::Sql`.

* **Instance metrics charts and inspectors across drivers (#93)** —
  PostgreSQL, MySQL/MariaDB, MongoDB, Redis, and SQL Server now expose
  live server metrics (time series) and tabular inspectors (sessions,
  processlist, currentOp, CLIENT LIST) through a new `InstanceCatalog`
  driver seam and two capability flags: `INSTANCE_METRICS` and
  `INSTANCE_INSPECTOR`. Each catalog publishes a driver-defined
  **Instance Overview** dashboard that opens read-only and can be
  cloned via "Save as editable" into a persisted, user-owned dashboard.
  Dashboards gain a new `Inspector` panel kind alongside `Chart` and
  `Divider`, persisted via `viz_dashboard_panels.panel_kind`
  (migration 014). Inspector rows expose driver-supplied row actions
  (e.g. *Terminate connection* / *Kill session*) gated by per-driver
  privilege probes (`pg_monitor`, `PROCESS` / `CONNECTION_ADMIN`,
  MongoDB `killOp`, Redis `CLIENT KILL`). Destructive actions route
  through `report_error_async` so failures land in the audit log with a
  correlation id, and every refresh timer (dashboard, chart, inspector)
  skips its tick when the underlying connection is gone so closing a
  connection no longer floods the toast layer.
* **External RPC drivers and auth providers can emit audit events (#157)**
  RPC-backed drivers (driver protocol v1.2, capability `AuditEmit`) and
  auth providers (auth-provider protocol v1.3, hello flag
  `audit_emit_opt_in`) can now write to the audit log over IPC by sending
  `EmitAuditEvent` frames as intermediate `done=false` responses. The host
  sanitizes every event: forces `actor_type`/`source_id` to new
  `ExternalDriver` / `ExternalAuthProvider` variants, fills `actor_id`
  with the registered RPC service ID, overrides connection context from
  `AppState`, enforces a per-source category whitelist (drivers:
  `Connection`/`Query`/`System`; auth providers: `Connection` only), and
  truncates `details_json` to the configured `max_detail_bytes`. A
  per-`socket_id` token-bucket rate limiter (100 events/minute, configurable)
  caps emission; overflow events are dropped silently — the IPC session
  is never blocked or errored — and counted on
  `AuditService::external_audit_dropped`. Older RPC peers that don't
  advertise the capability/flag remain silent.
- Centralized user-facing error reporting (`report_error` / `report_error_async` in `dbflux_ui_base`). Failures across mutations, file save, settings, and workspace actions now surface as a styled toast with a "View in Audit" action, increment a status-bar error badge, and emit a tracing event correlated with the audit row (#156).
- `EventRecord.correlation_id` is now populated from the `correlation_id` tracing field across all `dbflux` targets, regardless of whether the field is recorded via `%` (Display) or `?` (Debug) sigil (#156).
- **Tracing-to-audit bridge for centralized log capture (#154)** — A new `tracing-bridge` feature installs an `AuditLayer` subscriber in the `dbflux`, `dbflux_mcp_server`, and `dbflux_driver_host` binaries that funnels `tracing` events into the audit log through a bounded background queue with an atomic drop counter and in-flight gauge. A configurable `log_capture_min_level` audit setting (default `info`, persisted via migration 014) gates capture and updates the shared level atomic immediately. The layer applies a recursion guard, level gate, and summary truncation, and `AuditService::dropped_log_event_count()` exposes overflow drops.

### Changed

- Toast host applies a severity-aware throttle (capacity 5, refill 1 token / 2 s) to Warning and Info toasts so connection-storm noise does not bury the UI; Error and Fatal toasts bypass the throttle (#156).
- **Provider-neutral auth-profile edit seam (#155)** — The auth-profile edit path no longer carries AWS-specific types in the public core API. `dbflux_core::auth::edit` now exposes a provider-neutral `AuthEditSnapshot` (opaque `Arc<dyn Any>`), `AuthEditTarget`, and `AuthSaveOutcome`; the former `AwsEditFile` / `AwsEditSnapshot` / `AwsSectionHash` types moved to a private `dbflux_aws::edit` module, and `AuthProviderCapabilities` gained an optional `edit` field (serde-defaulted for backward compatibility). All three AWS providers were rewired to the neutral types with no behavior change.

### Fixed

- **Scripts-tab folders can be collapsed again** — Chevron clicks were routed through the connections tree only, so script-folder expansion lookups always returned `false` and every click tried to expand. Toggling now routes through the active tab's tree, propagates the override into the scripts tree state, and applies expansion overrides when building script items so collapses survive a refresh.
- **Syntax highlighting preserved across `AppStateChanged`** — `CodeDocument` re-applied the highlighter mode on every `AppStateChanged`, and `InputState::set_highlighter` clears the cached highlighter until the next render — wiping SQL coloring after running a query until the next keystroke. The document now tracks the last applied `editor_mode` and only re-applies the highlighter when it actually changes.
- **NULL rendered as an empty field in CSV export** — CSV export emitted the PostgreSQL `\COPY` sentinel `\N` for NULL, which most CSV consumers (Excel, Sheets, generic parsers) read as the literal string. NULL now exports as an empty field, the de facto CSV convention.
- **Inactive tab background no longer mismatches the tab bar.**
- **Multiline UPDATE/DELETE no longer falsely flagged as missing a `WHERE`** — The dangerous-query check matched only the literal substring `" where "`, so a `WHERE` placed on its own line (preceded by a newline rather than a space) was never found and the statement was wrongly reported as affecting all rows. Detection now strips single-quoted string literals (honoring `''` escapes) and matches `where` as a whitespace/paren-delimited token, fixing the false positive for both UPDATE and DELETE while still catching `where` text that only appears inside a value.

## [0.6.0-dev.10] - 2026-05-29

### Added

* **Saved charts, dashboards, and CloudWatch dashboard browsing (#152)** —
  Charts created from query results can now be saved, organized into
  dashboards, and reopened from the sidebar. CloudWatch connections gain a
  browse view that lists the account's dashboards as a read-only catalog so
  they can be inspected without round-tripping through the AWS console.
  The change also lands the workspace's PaneHandle/ResultPanel refactor:
  document tabs share a single closure-erased shell and a universal chrome
  row built from `ToolbarSegment`s, so the mode bar, filter bar, and
  refresh dropdown wrap responsively on narrow windows instead of pushing
  controls off-screen.
* **AWS profiles reflected live from `~/.aws` as source of truth (#149)** —
  AWS SSO, SSO-session, and shared-credentials profiles are now enumerated
  on demand from `~/.aws/config` and `~/.aws/credentials` via mtime-guarded
  caches, with a deterministic UUIDv5 identity per `(provider_id, name)` so
  reflected profiles are stable across launches without ever being stored.
  DBFlux holds zero AWS key material on disk (ADR-7): the static-credentials
  provider and its write-back paths are gone, and all `~/.aws/config`
  writers now go through the atomic locked primitive so concurrent edits
  can no longer truncate the file. Reflected entries surface in the auth
  picker as read-only and are distinguished from stored profiles by a new
  `AuthProfile.read_only` flag.
* **AWS SSO sessions as first-class auth profiles** — A new `aws-sso-session`
  auth provider models the `[sso-session NAME]` block of `~/.aws/config` as
  its own profile, and `aws-sso` profiles reference it via a generic
  `FormFieldKind::AuthProfileRef` dropdown instead of duplicating
  `sso_start_url` / `sso_region` inline. A new `expand_auth_profile_refs`
  pass merges the referenced session's fields into consumers at pipeline-input
  and MCP-resolution time (consumer overrides win). Settings now renders the
  selected session as inert text via `disabled_when_field_set`, and the
  AWS-profile importer routes `[sso-session …]` blocks into the session
  provider so re-importing matches sessions by name. Account/role dropdowns
  always probe with a session marker, so they populate as soon as a valid
  SSO session exists without forcing a new login.
* **Copy-to-clipboard export from the data grid (#153)** — The data-grid
  export menu now offers *Copy to clipboard* alongside *Save as file* for
  every text-friendly format. Binary export is deliberately disabled with
  guidance to use Hex or Base64 instead.

### Changed

* **Single Settings window across all entry points** — The four entry points
  that opened Settings (workspace action, auth-profiles deep link, Connection
  Manager section jump, sidebar footer) now all funnel through a shared
  `open_or_focus_settings` helper. Previously only the workspace path used
  `AppState::settings_window` for dedup, so the other three could stack
  duplicate Settings windows on top of each other.

### Fixed

* **Native file dialog now has a fallback path with user feedback (#153)** —
  `rfd::AsyncFileDialog::save_file()` returns `None` on both user-cancel and
  backend-failure, so on Linux systems without `xdg-desktop-portal` /
  `zenity` / `kdialog` the data-grid export and script *Save As* silently
  dropped the action. DBFlux now pre-flights the backend via a PATH probe;
  when none is available it writes to `~/.local/share/dbflux/exports/` with
  a non-clobbering filename, raises a warning toast, and emits a
  `result_export_fallback` audit event. When a backend exists, `None` is
  treated as a genuine cancel. Script *Save As* mirrors the same pattern
  and now surfaces write failures via toast instead of a silent log line.
* **Schema drift modal no longer fires on every SELECT for multi-FK tables
  (#151)** — The MySQL/MariaDB and MSSQL drivers built their foreign-key
  list from a `HashMap`'s values, whose iteration order is nondeterministic.
  Drift compares cached vs fresh foreign keys positionally and hashes the
  fingerprint in order, so two identical fetches in a different order looked
  like a change. Both drivers now use `ForeignKeyBuilder::build_sorted()`,
  matching the order the SQL `ORDER BY` already specifies and the Postgres
  driver's behavior.
* **MariaDB and InfluxDB profiles default to the correct config variants**
  — `default_db_config_for_kind` fell through to `default_postgres()` for
  MariaDB and InfluxDB, so a profile loaded via this fallback got a Postgres
  config while keeping its real kind; saving then persisted the mismatch,
  and connecting failed with *"Expected MySQL configuration"* for MariaDB.
  MariaDB now maps to `default_mysql()` and InfluxDB to `default_influxdb()`;
  the catch-all arm is removed so new `DbKind` variants fail to compile
  instead of silently degrading to Postgres.
* **AWS SSO login no longer hangs after a successful browser flow** — The
  cache lookup that polled for the new SSO token relied on
  `sha1(start_url)` as the filename. AWS CLI v2 actually names the file
  `sha1(session_name)` whenever the profile uses an `sso_session` block, so
  the fast path silently returned an unrelated, expired file and the polling
  loop spun forever even after `aws sso login` printed *"Successfully logged
  into Start URL"*. `find_sso_cache_contents` now always scans the cache
  directory and picks the newest entry whose `startUrl` field matches,
  covering both the legacy URL-keyed and modern session-keyed schemes.
* **Inline SSO login panel replaces the cross-window modal** — The login
  panel, verification URL, *Open Browser* / *Copy URL* / *Cancel* buttons,
  and the new `abort_sso_login` plumbing now live inside the Auth Profiles
  settings section. Cancel actually kills the running `aws sso login` child
  process via a shared abort flag and a per-profile abort registry. The
  stdout scanner was also rewritten with a bounded `recv_timeout` loop so
  PKCE-flow URLs (which never print `user_code=`) are surfaced immediately
  instead of blocking indefinitely.

## [0.6.0-dev.9] - 2026-05-26

### Added

* **Metric picker rail tab for chart documents** — CloudWatch metric charts now
  open with an interactive picker rail (320 px overlay) that lets users browse
  namespaces, metrics, and dimension combinations fetched live via
  `ListMetrics` pagination. Selecting a metric and pressing Apply swaps the
  chart's data source and auto-runs the query. Results are cached for the
  session by `MetricCatalogCache`. No driver names or categories are hardcoded
  in the UI layer; the Metric tab is gated solely on the generic
  `METRIC_CATALOG` capability bit (#96).
* **Time-range macros for InfluxQL and Flux** — user-written InfluxDB queries
  can opt into UI-driven time-range substitution via Grafana-style tokens
  (`$timeFilter`, `$__from`, `$__to` for InfluxQL; `v.timeRangeStart`,
  `v.timeRangeStop` for Flux). Substitution happens at the execution
  chokepoint in both `CodeDocument` and `ChartDocument`; the InfluxDB driver's
  inject-when-absent path is skipped when macros are present so they take
  precedence without double-injection. Queries without macros keep today's
  byte-for-byte behavior. Documented in the driver README (#119).

### Changed

* **Driver-owned connection form definitions** — Built-in connection form
  schemas moved out of `dbflux_core` and into their owning driver crates.
  Core now keeps only the generic `DriverFormDef` primitives and helper
  builders, while the connection manager reads forms through the existing
  `DbDriver::form_definition()` seam. This removes driver-specific defaults,
  URI placeholders, tab layouts, and conditional field rules from core with
  no connection-manager behavior change (#140).
* **Dialect-specific language services leave core** — SQL Server's
  `TSqlLanguageService` now lives in `dbflux_driver_mssql`, matching the
  MongoDB and MySQL driver-owned language-service pattern. Core retains the
  generic `LanguageService` seam and shared SQL helpers, but no longer exports
  the T-SQL-specific implementation (#129).
* **MongoDB and Redis dangerous-query detection moved to drivers** — MongoDB
  and Redis dangerous-operation classifiers now live in their driver language
  services, and code execution asks the active connection's language service
  to classify dangerous queries. Core still owns the shared
  `DangerousQueryKind` type and SQL classifier, but no longer exports
  Mongo/Redis-specific detection helpers (#139).
* **Sidebar collapses single-database wrapper** — Connections whose driver
  exposes exactly one database (CloudWatch's `logs`, DynamoDB's default
  region, single-file SQLite, etc.) no longer render the redundant database
  level. Child nodes (Collections, Metrics, Tables) attach directly under the
  connection node. Multi-database drivers (Postgres, MySQL, MongoDB) are
  unaffected — the wrapper still discriminates between databases (#131).
* **CloudWatch metric catalog hardening** — The `RealCloudWatchClient` adapter
  now reuses a single long-lived Tokio runtime across `list_metrics` calls
  (previously a new runtime was constructed per call, wasting file descriptors
  during full-namespace sweeps). The namespace sweep is also bounded at 50
  pages (~25,000 metrics) to cap the worst case on very large AWS accounts; the
  cap is documented in the driver README. A future change will replace the cap
  with full timeout + cancellation infrastructure (#96).
* **Sidebar metric leaves dedupe by metric name** — On accounts with
  per-instance metric explosion (e.g. AWS/EC2 with 1000 instances) the
  CloudWatch driver returns one `MetricDescriptor` per `(metric_name,
  dimension_combo)` pair. The sidebar now collapses these into one leaf per
  distinct `metric_name`; dimension refinement still happens inside the chart
  document's picker rail (#96).
* **Metric chart entry point moved to the sidebar** — Clicking a metric leaf
  in the connection sidebar (Metrics > Namespace > Metric) opens a chart
  pre-populated with defaults (Average statistic / 5 min period / aggregate
  across all dimensions) and immediately executes it. The picker rail opens
  alongside for refinement of dimensions, period, and statistic. Duplicate
  clicks on the same metric leaf focus the existing tab (#96).
* **Centralized `TimeRangePanel` custom-picker rendering** — A new
  `render_custom_picker_row` helper (and `CustomPickerSlots` for hosts that
  need per-slot decoration) is shared across `ChartDocument`, `CodeDocument`,
  the data-grid chart toolbar, and the audit document. The data-grid chart
  toolbar gains the custom date/hour/minute picker that previously was
  missing under "Custom…", and audit migrates off the last hand-rolled
  row. Behavior-preserving — public accessors and emitted events are
  unchanged (#121).
* **Chart toolbar wraps on narrow viewports** — The shared chart toolbar
  used by `ChartDocument` and `DataGridPanel` switched from a single
  non-wrapping flex row to the codebase's responsive pattern
  (`flex_wrap` + `gap_x/gap_y`, `min_h(34px)`). Trailing controls (TYPE
  chips, Stats / PNG / Save) no longer push off-screen when the document
  is narrow; rows grow downward instead of clipping (#136).
* **Stats rail gains an in-rail close affordance** — `ChartDocument`'s Stats
  rail now renders a header with a STATS title and an `×` close button so
  users can dismiss it without hunting for the toolbar toggle (#136).

### Removed

* `open_metrics_chart` workspace action and command-palette entry — the sidebar
  tree is now the single entry point for metric charts.
* `ChartDocument::new_empty_metric_chart` constructor — replaced by
  `new_with_source` with a pre-built `MetricSource` and `setup_metric_picker`.

### Fixed

* **Command palette keyboard navigation follows visual sections** — Filtered
  command-palette items now sort by rendered section order before match score,
  so Up/Down navigation moves through Connections, Commands, Charts, Tables,
  and Scripts exactly as displayed. Pressing Up from the first item in a
  section now lands on the previous visible section instead of jumping within
  the score-sorted backing list (#143).
* **Modal sizing & button overflow** — three confirm dialogs (Run entire
  script, Dangerous query, sidebar Delete/Drop) now use the shared
  `ModalShell` primitive with consistent widths and a dedicated footer
  button row. Buttons no longer overflow into the body, and the
  Drop Database / Delete confirms no longer render at half the size of
  the other confirm modals (#130).
* **Chart axis tick density on wide/tall plots** — Three targeted
  adjustments to the chart engine raise tick density without over-ticking
  small charts: `NICE_TIME_STEPS_MS` gains 2h / 3h / 12h / 2d / 3d entries
  (a 3-week range with 12 target ticks no longer collapses to 3 weekly
  ticks), the X-tick clamp floor drops from 4 to 3 so ~400px charts can
  render 3 ticks, and the Y-axis target switches from a build-time
  constant of 5 to a render-time `(plot_h / 60).clamp(3, 12)` that mirrors
  the existing X dynamic path (covers line / area / StackedBar / log).
  PR #123's dynamic edge-label padding is preserved (#132).
* **`ChartDocument` Stats rail toggle now actually renders** — The toggle
  state machine was complete but `render_chart_content` had no
  `ChartRailTab::Stats` branch, so clicking Stats appeared to do nothing.
  The rail now renders for query-result, saved-chart, and CloudWatch
  metric hosts with SERIES / STATS / WINDOW / SOURCE sections matching
  `DataGridPanel`. Closes #133 (#136).
* **MySQL editor diagnostics no longer flag DCL statements** — the MySQL
  driver was using the generic `SqlLanguageService` (tree-sitter-sequel
  / ANSI SQL), which chokes on `CREATE USER 'u'@'h' IDENTIFIED BY '…'`,
  `GRANT … TO 'u'@'h'`, `FLUSH PRIVILEGES`, etc., surfacing spurious
  "Unexpected …" errors. A new `MySqlLanguageService` (mirrors the
  MongoDB pattern) overrides `Connection::language_service()` to return
  empty editor diagnostics — the server stays the source of truth.
  MariaDB shares the impl and is covered automatically. Closes #126
  (#128).
* **`TimeRangePanel` window preserved against stale source-input
  clobber** — The result-panel chart toolbar's panel emitted
  `TimeRangeChanged` on every preset click, but `run_query_text` then
  unconditionally rebuilt `exec_ctx.source` from the once-populated
  `source_*_input` text fields, silently overwriting the panel's
  selection. A new `pending_window_override` on `CodeDocument` carries
  the authoritative panel bounds through a pure
  `resolve_source_context` helper that gives the override precedence
  over the input-driven fallback (and suppresses input validation
  errors when an override is present). `ChartDocument` was unaffected
  (no dual source of truth) (#124).
* **X-axis edge labels no longer clipped on charts** — the label paint
  loop centered labels on tick screen-X with no right-bound clamping,
  and the fixed `MARGIN_RIGHT = 16` did not reserve space for label
  overhang. A pre-shape pass in the paint closure now measures label
  widths and derives effective horizontal padding as
  `max(MARGIN_*, max_label_w / 2.0)` (base margins as a floor), with a
  symmetric left-edge guard. The Y-tick column tracks the effective
  left pad so it stays flush with the plot. Extracted as
  `effective_x_label_padding` with 6 unit tests (#120).
* **`ChartDocument` custom-range apply race** — `apply_custom_range`
  now sets `pending_time_window` and `pending_chart_reexecute`
  synchronously from the validated `(start_ms, end_ms)` returned by
  the panel, instead of waiting for the deferred `TimeRangeChanged`
  subscription. The subscription still fires for `selected_time_range`
  mirroring, but re-execution is no longer gated on its delivery
  timing (#121).
* **Connection + sidebar UX batch** — cancelling a connect task now
  also clears the profile-level pending-operation entry so the
  sidebar exits the "(connecting...)" state immediately. Editing a
  currently-connected profile surfaces a "Reconnect now / Later"
  toast; the edit always persists and the live session refreshes
  only on opt-in. Reopening Settings after closing it no longer
  wastes the first click (stale window handle cleared on close and
  on focus failure). Ctrl+click in the sidebar now seeds the
  keyboard-focused item into the multi-selection before toggling,
  matching the visual cursor (#145).
* **Row inspector follows the active tab and selection** — the
  workspace inspector rail used to be a singleton with no per-tab
  state, so switching tabs left a previous table's inspector
  rendered against the new tab's chrome. `DataGridPanel` now
  remembers `(row, col)` when the inspector opens and re-snapshots
  the row on tab activation, result refresh, and selection
  changes (click / arrow keys). Rows that fall out of bounds after
  a refresh close the rail cleanly. Explicit dismissal (× / ESC)
  drops the cached coords so the rail stays closed on return.
  Inspector column-name and value cells now share a flex layout
  (140px / 220px basis) with ellipsis truncation, so long names
  no longer wrap and resizing the rail redistributes width across
  both columns. `Ctrl+A` / `Cmd+A` inside an inline cell editor
  now selects the input text instead of all table rows (#145).

## [0.6.0-dev.8] - 2026-05-23

### Added

* **Audit event charts** — the Audit document now has a Table/Chart view
  toggle that visualizes the currently filtered audit events as counts
  over time, with one series per group value (grouped by category, outcome,
  or level). The chart honors the document's active time range and
  auto-refresh. Charts are ephemeral (a view mode, not a saved artifact).
* **Logarithmic Y axis for charts** — charts can switch the Y axis between
  linear and logarithmic (log1p) scale, so large spikes no longer flatten
  the rest of the data. Exposed in the audit chart toolbar.
* **CloudWatch metric charts** — CloudWatch connections can graph real
  metrics (via `GetMetricData`) as a time-series chart. An "Open Metrics
  Chart" command is available whenever the active driver advertises the
  generic metric-series capability; the chart refreshes over the active
  time window. The metric is currently fixed (AWS/Lambda Invocations,
  average over 5-minute periods); an in-app metric picker is a follow-up.
* **Generic `ChartDataSource` seam (W0)** — a driver-agnostic chart data
  trait that the audit and CloudWatch chart features both consume. The UI
  never branches on driver identity; charts are wired through metadata and
  capabilities.

### Changed

* **Charts respond to the active theme** — chart canvas chrome (gridlines,
  tick labels, crosshair, hover dot, readout overlays) and chart overlays
  (legend, axis bar, point inspector, picker) now route through a new
  `semantic::ChartColors` palette resolved per active theme. The Light
  theme no longer renders the dark series palette over a light canvas;
  Mirage and Dark use theme-driven series colors via the engine's
  `theme.chart_1..chart_5`. Dark series colors remain byte-identical to
  prior releases. Three deliberate Dark chrome divergences (gridlines via
  `theme.border`, tick labels via `theme.muted_foreground`, hover-dot
  background via `theme.background`) carry through and were validated by
  visual QA.
* **Document toolbar styling unified** — every document type's toolbar now
  uses the same shared primitives (icons, separators, spacing) so the look
  is consistent across SQL editors, chart documents, audit views, and the
  data grid.
* **UI split into six layered crates** — the monolithic `dbflux_ui` crate
  was split into `dbflux_components` (domain-free leaf), `dbflux_ui_base`
  (events/keymap/AppState seam), `dbflux_ui_document` (tabs, panes, all
  document types), `dbflux_ui_windows` (settings + connection manager),
  `dbflux_ui_sidebar`, and a thin `dbflux_ui` integrator. Per-driver
  feature flags no longer live on UI crates (they belong to `dbflux_app`,
  which registers drivers). Incremental rebuilds are noticeably faster.
* **Design tokens consolidated across every UI crate** — every UI crate
  now consumes the centralized `dbflux_components::tokens` scale
  (`Spacing`, `Borders`, `Widths`, `ChartGeometry`) and routes banner
  colors through a single `semantic::BannerColors`. Each crate is locked
  by a source-scanning guardrail test that prevents regressions. Chart
  factory files (`axis_bar`, `point_inspector`, `legend`) sit under the
  guardrail; only `chart/engine.rs` stays exempt for canvas geometry math.
  Behavior-preserving.
* **`dbflux` binary dependency cleanup** — the binary's `Cargo.toml` no
  longer declares the driver/runtime crates as direct optional deps; they
  are activated through `dbflux_app/<feature>`. Feature relays unchanged
  from a user perspective; `--features sqlite,…,lua,aws,mcp` continues to
  work identically.

### Fixed

* **Audit row detail expanded full-width with custom range inputs
  visible** — the Audit document's row detail panel now spans the full
  width of the document and the custom date-range inputs are no longer
  clipped behind toolbar chrome.
* **Audit SQLite "database is locked" errors under contention** — the
  audit store now sets a 5s `busy_timeout` when opening its connection.
  Since the audit database shares a WAL file with `StorageRuntime` (and
  tests may race on a shared temp path), concurrent openers previously
  failed immediately with `SQLITE_BUSY` instead of waiting; they now
  serialize. Fixes intermittent test failures in the MCP governance
  suite.

## [0.6.0-dev.7] - 2026-05-21

### Fixed

* **Focus shortcuts on macOS/Windows** — `Ctrl+Shift+1..4` (Focus
  Sidebar / Editor / Results / Tasks) now fire on every platform. GPUI
  normalizes `Shift`+digit chords at the platform layer (e.g. macOS
  delivers `Ctrl+Shift+2` as `@` with `shift=false`), so the literal
  `KeymapStack` matchers never matched the runtime keystroke. The four
  shortcuts are now registered as native GPUI key bindings, which GPUI
  normalizes per platform/layout at registration time. The `KeymapStack`
  entries are retained solely as the command-palette shortcut-label
  source.
* **DriverCapabilities bit collision** — `MULTI_STATEMENT` and `ROUTINES`
  were both defined as `1 << 47` in the same bitflags, so a driver
  advertising one silently advertised the other. `MULTI_STATEMENT` now
  occupies bit 48, with a regression test asserting the bits are
  distinct.
* **DynamoDB upsert capability** — `MutationCapabilities.supports_upsert`
  was `false` even though the driver implements single-item upsert
  (`PutItem`) and only rejects `many + upsert`. The flag is now `true`,
  so the MCP write tool no longer rejects a supported operation.

## [0.6.0-dev.6] - 2026-05-20

### Added

* **Multi-statement script execution** — running a buffer with no active
  selection now offers to execute the whole script (multiple
  `;`-separated statements) behind a "Run entire script (N statements)?"
  confirmation, on drivers that advertise the new
  `DriverCapabilities::MULTI_STATEMENT` flag. `QueryLanguage` splits
  SQL-family buffers while skipping separators inside strings,
  identifiers, line/block comments, and PostgreSQL dollar-quoted bodies;
  non-SQL languages stay single-statement. PostgreSQL routes batches
  through the simple query protocol (batched columns are untyped text);
  MySQL/MariaDB and SQLite split client-side and run each statement
  through the typed prepared path (also fixing SQLite silently executing
  only the first statement); MSSQL already executed batches natively.
  Each result set renders in its own result tab. The seam is
  driver-agnostic — the UI gates on the capability flag, never on driver
  identity.
* **Stored Procedures / Routines folder** — a capability-gated Routines
  folder now appears under schema nodes in the sidebar, gated on the new
  `DriverCapabilities::ROUTINES` flag (never on driver id). Core exposes
  `RoutineInfo` / `RoutineKind` keyed on the engine-provided
  `specific_name` for overload-safe node identity, plus
  `Connection::schema_routines` / `routine_definition` with default
  empty implementations so non-supporting drivers fall back gracefully.
  PostgreSQL (via `pg_proc`/`pg_get_functiondef` with an aggregate/window
  fallback), MySQL/MariaDB (via `information_schema.ROUTINES` +
  `SHOW CREATE`), and SQL Server (via `sys.objects` +
  `OBJECT_DEFINITION`) implement listing. Clicking a routine opens a
  read-only `CodeDocument` (editor disabled, completion off, mutating and
  execution toolbar buttons hidden) that round-trips across session
  restore.

### Changed

* **Workspace document architecture refactor** — the closed
  `DocumentHandle` enum that previously gated every document type was
  replaced with a `PaneHandle` closure-erasing shell. Adding a new
  document type now requires only a new `<name>/pane.rs` and one
  `open_<name>` function in `workspace/actions.rs`; no changes to
  `workspace/mod.rs`, `tab_manager.rs`, `tab_bar.rs`, or `handle.rs`.
  Introduces `DocumentKey` for tab deduplication (replaces the six
  `is_*` methods), a unified `DocumentEvent` (replaces four per-document
  event enums), and a universal `ResultPanel` + `ViewHandle` chrome host
  with a `ToolbarSegment` slot system (`Left | Center | Right` +
  `index`, `flex_wrap` row) for filter bars, axis bars, range chips,
  and similar view-provided controls. `handle.rs` reduced from 486 to
  29 LOC; `audit/mod.rs` reduced from 3454 to 1628 LOC. No new
  dependencies, no functional regressions, 2169 tests pass.
* **Chart-specific icons across chart surfaces and the result mode bar**
  — added `ChartSpline`/`ChartArea`/`ChartColumnBig`/`ChartBar`/
  `ChartPie`/`ChartNetwork` icons (with a `for_chart_kind` helper) and
  replaced generic placeholders: the chart tab and the "Chart this
  query" menu/editor button now use `ChartSpline`, the chart toolbar
  Stats button uses `ChartBar`, and the Data | Chart | JSON result-view
  mode bar gains per-mode icons.

### Fixed

* **Result mode bar appears in CodeDocument query results** —
  `DataGridPanel::available_result_view_modes` no longer gates on the
  currently active mode, so the Data | Chart | JSON bar now renders the
  moment a `QueryResult` arrives (instead of only after the user
  manually switched away from Table). Regression introduced earlier in
  the workspace-view refactor.

## [0.6.0-dev.5] - 2026-05-20

### Added

* **Microsoft SQL Server driver** — first-class SQL Server support
  built on `tiberius`, with TLS modes (`off`, `on`, `required` +
  `trust_server_certificate`), SSH tunnel and SQL Browser named-
  instance routing, full multi-schema introspection (`hr`, `sales`,
  `dbo`, …), CRUD via `OUTPUT INSERTED.*` / `OUTPUT DELETED.*`,
  `OFFSET ... FETCH NEXT` paging, and cooperative query cancellation
  via side-channel `KILL <spid>` with automatic session restore and
  active-database recovery. `ColumnKind` is wired across every
  `tiberius::ColumnType` so MSSQL results integrate with chart
  auto-detection.

### Fixed

* **Long text wraps in toasts, banners, and the delete-confirmation
  modal** — long error strings and titles previously overflowed past
  the card edge instead of wrapping. The flex chain inside the card
  is now configured so titles and subtitles wrap within the
  container's `max_w`.
* **Delete-confirmation popup no longer duplicates the dedicated
  delete modals** — when `ModalDeleteConnection` or
  `ModalDropTable` is open, the generic confirmation popup is now
  suppressed so users don't see two overlapping delete dialogs.
* **Connection profile add / remove / update now persist on disk** —
  removing a profile failed to delete its row because `save_profiles`
  was upsert-only, and add / update relied on an MCP-side persist
  hook so changes were lost on builds without MCP. `app_state` now
  calls the storage repository directly on every mutation.

## [0.6.0-dev.4] - 2026-05-19

### Fixed

* **Data grid column header prioritizes the column name** — the name,
  PK/FK badges, and type chip were equal-weight siblings, so long type
  labels (e.g. MySQL's raw `MYSQL_TYPE_VAR_STRING`) pushed the column
  name out of view. The name is now the primary affordance rendered with
  the standard foreground and never ellipsized, while the type label and
  PK/FK badges share a single muted styling and shrink first. MySQL now
  maps protocol types to canonical SQL labels (`VARCHAR`, `BIGINT
  UNSIGNED`, `DECIMAL(p,s)`, …) and DynamoDB infers a label from the
  first sampled item instead of showing the literal `"DynamoDB"`.
* **Pending inserts are committed on save** — `request_save_all` emitted
  virtual row indices for pending inserts while the commit path looked
  them up by array index, so on any table with existing rows the save
  aborted silently with no error. Inserts now persist regardless of the
  base row count.
* **Chart engine plots Decimal and Bool columns** — `extract_f64` only
  handled `Value::Int`, `Value::Float`, and timestamp-typed `Value::Text`,
  silently dropping `Value::Decimal` and `Value::Bool`. Columns whose
  `ColumnKind` is numeric (e.g. PostgreSQL `NUMERIC`, MSSQL `DECIMAL`,
  MSSQL `BIT`) now render correctly instead of producing an empty series
  with no error.
* **PostgreSQL array columns accept inserts and updates** — saving a row
  into a `text[]` / `int4[]` / etc. column failed with `expression is of
  type jsonb` because the dialect emitted `'<json>'::jsonb` regardless of
  the destination type. Per-column type metadata now flows from the UI
  data grid and MCP write tools through `RowInsert`/`RowPatch`/
  `SqlUpdateRequest`/`SqlUpsertRequest` to the dialect, which emits
  `ARRAY[...]::elem[]` for array columns and keeps `::jsonb` for JSON
  columns. The IPC wire format stays backward compatible via serde
  shims, so older driver peers keep working.

## [0.6.0-dev.3] - 2026-05-16

### Added

* **Chart engine** — first-class time-series charts across the
  workspace. Results in the data grid gain a Chart mode with an axis
  bindings bar (X / Y / Group By / Aggregate), a shared toolbar
  (range, refresh, window, points, stats, PNG, save), LTTB
  decimation, axis tick labels, a user-toggleable legend, and a
  crosshair readout with nearest-sample lookup. Charts can be saved
  and reopened from the command palette.
* **`ChartDocument`** — standalone chart document opened via "Chart
  this query" from a data grid context menu. Owns its own time-range
  panel, refresh dropdown and execution loop; the query is fixed for
  the document's lifetime.
* **`ColumnKind` metadata** — every driver now reports per-column
  semantic kind (Timestamp, Numeric, Tag, etc.) used by chart
  detection. Wired across Postgres, MySQL, SQLite, MongoDB, Redis,
  DynamoDB, CloudWatch, OpenSearch, Cypher, and InfluxDB.
* **InfluxDB driver** — InfluxDB v1 (InfluxQL) and v2 (Flux) support
  with full query, chart, and metadata integration.

### Changed

* **Branding** — adopted the new DBFlux mark from the design system
  across the application chrome.

## [0.6.0-dev.2] - 2026-05-14

### Fixed

* **SQL editor diagnostics no longer flag PostgreSQL dollar-quoted
  blocks** — valid `DO $$ ... $$;` anonymous code blocks and other
  `$tag$`-quoted bodies were marked with spurious syntax errors because
  the bundled tree-sitter SQL grammar does not understand dollar
  quoting or PL/pgSQL. Parse diagnostics are now skipped when the query
  contains a closed dollar-quoted block.

## [0.6.0-dev.1] - 2026-05-14

### Changed

* **Platform-aware keybindings** — application-level shortcuts now use
  Cmd on macOS and Ctrl on Linux/Windows: command palette
  (`Cmd/Ctrl+Shift+P`), new/close/switch tab, run query, save, open
  script/history, export results, toggle sidebar, audit viewer, and
  Results cell copy. vim-style navigation (`Ctrl+h/j/k/l`, `Ctrl+u/d`)
  and `Ctrl+Tab` / `Ctrl+Shift+Tab` stay literal Ctrl on every platform,
  along with focus shortcuts (`Ctrl+Shift+1..4`) that would clash with
  macOS screenshot bindings and `Ctrl+M` which would clash with
  window-minimize on macOS. Inline data-table commands (Copy, Save row,
  Select all, Undo/Redo) use GPUI's `secondary-` modifier so they pick
  the right key per platform. Command-palette shortcut labels and the
  SQL editor / save-row hints now reflect the platform modifier. Closes
  #63.

## [0.6.0-dev.0] - 2026-05-12

### Features

* **Workspace-level inspector rail** — row inspector promoted to a
  workspace-wide rail and migrated off the per-document overlay (#52).
* **Nix prebuilt-binary package** — `pkgs.dbflux` is now a prebuilt
  binary fetched from the matching GitHub Release (pinned by
  `nix/release-info.nix` and built via `nix/binary.nix`), with a
  fallback `pkgs.dbflux-source` for compiling locally. The flake also
  exposes `overlays.default` so downstream flakes can consume the
  package directly.

### Chores

* Adopt trunk + short-lived release-branch model: add `CONTRIBUTING.md`,
  label-aware PR/issue templates, and `docs/RELEASE.md` documenting the
  cut and tag procedures.
* Release workflow publishes stable tags (`vX.Y.Z`) directly and marks
  `-dev.N` / `-rc.N` tags as prereleases.
* Cherry-pick discipline now requires removing the corresponding entry
  from main's `[Unreleased]` block after the picked commit lands on the
  release branch, and the cut procedure verifies the release workflow
  has the `Classify release` step before tagging.

## [0.5.6] - 2026-05-13

### Fixes

* Toast bubble no longer overflows the screen when the subtitle is long.
  The title and subtitle now stack vertically inside a `flex_1 min_w_0`
  column so the subtitle wraps within the card's `max_w` (raised from
  26rem to 28rem) instead of pushing the whole toast past the workspace
  edge. The card also calls `.occlude()` so clicks on its empty area no
  longer fall through to the sidebar or document underneath.

## [0.5.5] - 2026-05-13

### Fixes

* Schema-drift preflight no longer reports phantom "all columns removed"
  for queries whose table lives outside `public`. The fresh fetch is now
  steered to the right schema via a layered precedence (query qualifier
  → cached `TableInfo.schema` → editor toolbar's schema → `public`
  fallback), and the checker defensively skips any entry whose driver
  lookup returns zero columns — preventing the empty `TableInfo` from
  poisoning the autocomplete and table-detail caches via the
  "Refresh & re-run" path.

### Chores

* Pin EOL to LF via `.gitattributes` (`* text=auto eol=lf`) so
  `cargo fmt` no longer desyncs Windows working trees that default to
  `core.autocrlf=true`.

## [0.5.4] - 2026-05-13

### Fixes

* Results table horizontal trackpad / wheel scroll now respects the
  platform sign convention (macOS "natural scrolling" preference,
  Linux / Windows scroll direction) and the body shifts on the same
  frame as the scrollbar, removing the one-frame lag that read as
  jitter during trackpad momentum. Follow-up to #60.

## [0.5.3] - 2026-05-13

### Features

* Ctrl+C / Cmd+C now copies the selected cell (or range) from the Results
  grid to the clipboard, matching the right-click → Copy behavior.

### Fixes

* Results table now scrolls horizontally with trackpad / Magic Mouse
  gestures and `Shift+Wheel`. The horizontal scroll handle is owned by
  a 1px phantom scroller so the scrollbar widget can drive it, which
  meant horizontal wheel deltas landing on the header or body were
  dropped; the table now forwards those deltas to the handle, and the
  vertical-only uniform list is restricted to its axis so GPUI's
  built-in delta.x → delta.y fallback no longer double-scrolls on
  shift+wheel (#58).

## [0.5.2] - 2026-05-13

### Fixes

* Results data grid shows the horizontal scrollbar immediately when
  the columns are wider than the viewport. gpui-component scrollbars
  render fully transparent at idle and only fade in after a scroll
  event; the horizontal axis is driven by a 1px phantom scroller that
  never receives the wheel, so previously the bar stayed invisible
  until the user arrowed past the right edge. The horizontal scrollbar
  is now configured with `ScrollbarShow::Always`.

## [0.5.1] - 2026-05-12

### Features

* Logger now initialises at the very start of `run_gui()` so startup
  diagnostics (IPC socket binding, auth token init) reach the log sink.
  Setting `DBFLUX_LOG_FILE` redirects all `log::*!` output to the given
  file in append mode — useful on Windows where the GUI subsystem hides
  stderr.

### Fixes

* SQL editor keeps focus after dismissing the completion popup with Esc.
  gpui-component's `CompletionMenu::hide` clears the menu but the
  follow-up re-render drops `window.focus` even though the input still
  owned it synchronously; the editor pane now re-focuses its input on
  the next tick so typing keeps working.

## [0.5.0] – 2026-05-11

### Features

* **Design system foundation** — new `dbflux_components` crate with a complete
  design-system token scale (`AppStyle` Compact / Default density tiers,
  semantic color tokens, density accessors threaded through `Button`,
  `Dropdown`, `PanelHeader`, `Surface`, `Badge`, `FocusFrame`, `Text` and the
  whole typography stack). The Style is persisted in `general_settings` and
  selectable from a new Style dropdown in General settings. Ayu Mirage joined
  Ayu Dark as a first-class theme.
* **Hi-Fi design bundle applied across the app** — workspace chrome refresh
  (Linux CSD titlebar with breadcrumb, doc-tab dirty dot, pulsing status bar),
  sidebar (compact tab strip, magnifier-prefix search, no double border,
  `StatusDot` per row, single compact footer with connected/idle count),
  data grid with column PK/FK badges and row-state colors, paginator
  `‹ N / Total ›`, schema-drift detection with modal, command palette
  redesign (grouped sections, `Chord` shortcuts, deep-Ayu-Dark background),
  settings navigation (uppercase XS group headers, `warning_bg`-tinted active
  item, keybinding rows with `Chord` + conflict banner), audit document
  6-column grid with `BannerColors` LVL chips, empty workspace state with
  shortcut chords.
* **New shared primitives** — `StatusDot`, `BannerBlock`, `TypeToConfirm`,
  `Chord`, `KbdBadge`, `SegmentedControl`, `FilePicker`, `Logs` icon,
  `RowColors`/`BannerColors`/`StatusDotPalette` token families, `Anim` and
  `Widths`/`Shadows` constants.
* **Rich Toast system** — explicit `Toast::xxx(title).subtitle(...).body(...)`
  `.details(...).code_block(...).progress(...).action(...).collapsible()`
  `.push(cx)` builder with auto-dismiss policy per variant, action buttons,
  collapsible details, and a 4 px left accent stripe. All ~100 call sites
  migrated to the explicit builder; the old `cx.toast_xxx(msg, window)`
  trait removed. SQL execution errors render rich with `FormattedError`
  (subtitle = SQLSTATE, body = message, code_block = HINT, "Copy" action).
* **Row Inspector overlay** — 320 px floating panel with PK/FK indicators,
  FK forward-resolution (issued against the per-database connection so it
  works on Postgres' connection-per-database model), inline wrapping for
  long FK headers and resolution errors, drag-mask resize (240 – 1280 px),
  scroll containment, and a working `×` close button.
* **Connection manager rebuild** — driver picker as a grouped, alphabetical
  4-col card grid with `/`-focusable filter input and 2D keyboard
  navigation; per-driver SSL modes via `SegmentedControl` declared by each
  driver's metadata; cert paths chosen via `FilePicker`; SSH passphrase
  prompt with 60 min in-memory remember; enriched test-connection
  `BannerBlock` (engine version, RTT, server time, SSL ciphersuite).
* **Driver metadata expansion** — new `DatabaseCategory::LogStream` (with
  CloudWatch reclassified to it and using the new `Logs` icon),
  `DeploymentClass` enum (Self-hosted / Embedded / Cloud-managed) surfaced
  in Settings → Drivers, per-driver `SslModeOption` lists and
  `SslCertFields` capability. MongoDB and Redis gained TLS support
  (`CombinedPemFile` helper concatenates cert+key for MongoDB).
* **Schema-aware features** — `SchemaCache::dependents` cache, per-driver
  `fetch_dependents`, `referenced_tables`, `fetch_row_by_pk`,
  `test_connection_rich` on `Connection`; `SchemaFingerprint` for drift
  detection.
* **Built-in CloudWatch Logs integration** (#43).
* **External RPC auth providers reach AWS parity** — runtime registration
  over RPC, login-capable providers with device-URL flow surfaced in the
  shared login modal, opaque `AuthSessionDto.session_data` round-trip, and
  generic `DynamicSelect` form fields whose options are fetched through the
  new `FetchFieldOptions` IPC method. Auth-provider IPC reaches v1.2 with a
  `secret_dependency_opt_in` manifest flag; `Password`-typed values are
  stripped from option requests by default. AWS SSO Account ID / Role Name
  dropdowns now travel through the generic path — no provider id is
  hard-coded in the Settings panel anymore. The Provider selector is a
  single dropdown over the full registry (built-in + RPC-discovered).
* **Sidebar batch delete** — multi-select rows and delete them in one action.
* **RPC services foundation** — formalised driver and auth-provider service
  kinds, shared bootstrap, and negotiated API-version contracts at startup.

### Fixes

* `FetchOptionsError::SessionExpired` and `NeedsLogin` now surface a visible
  per-field re-login hint and provider-level banner instead of being
  silently logged.
* `RefreshTrigger::Manual` only fetches on cache miss — no more refetch on
  every render.
* MySQL connection configs are preserved across reloads.
* The data grid keeps CRUD actions available on empty tables.
* `nix develop` is back to a working state.
* Sidebar tree survives degraded storage loads and no longer overwrites a
  broken connection tree.
* PostgreSQL `GRANT` statements no longer surface false-positive
  diagnostics in the editor.
* Editor focus is preserved after running a query.

### Chores

* CI now runs DynamoDB live integration tests.
* Workspace-wide rustc/clippy lints (warn level) opted into by all driver
  crates; `rustfmt.toml` baseline added.
* Repo-specific workflow skills added for contributor automation.

---

## [0.4.6] – 2026-04-18

### Features

* Add sidebar refresh and drop actions for schema nodes (#29)
* Add a new app icon and fix the About section display

### Fixes

* Improve small app icon rendering in packaging assets
* Persist all pending row changes on save, not just the first (#28)
* Keep column resize drag active until mouse release

### Improvements

* Restore plural-aware delete confirmation copy for multi-row deletes after the main/dev merge

---

## [0.4.5] – 2026-04-15

### Features

* Register value providers for AWS static credentials auth (#22)
* Sign .deb/.rpm packages natively and make GPG signing always-on (#6, #23)

### Fixes

* Use UUID for temp SQLite path to avoid parallel test lock contention
* Align audit filter controls and multi-select behavior (#21)

### Improvements

* Unify MCP audit with app-wide audit system (#20)

---

## [0.4.4] – 2026-04-09

### Features

* Add MCP `create_type` support (#15)

### Fixes

* Add Linux client-side window decorations in the UI (#14)
* Expand command palette global search behavior (#17)
* Preserve MongoDB SRV URIs in URI mode (#19)

### Improvements

* Add a pull request template to standardize change summaries and validation details

---

## [0.4.3] – 2026-04-08

### Features

* Implement cooperative query cancellation for MongoDB driver (#11)
* Wire proxy tunnels into the connect pipeline (#12)

### Improvements

* Update README with screenshot and installation options

---

## [0.4.2] – 2026-04-06

### Features

* Add deb and rpm package generation to Linux release workflow

---

## [0.4.1] – 2026-04-05

### Fixed

* PKGBUILD now downloads pre-built Linux binaries from GitHub Releases instead of compiling from source
* Release artifacts (tar.gz, AppImage) now include LICENSE files

---

## [0.4.0] – 2026-04-05

### Architecture

* Codebase split into `dbflux_app` (pure domain, no GPUI) and `dbflux_ui` (all GPUI/UI code); `dbflux` binary is now a thin shell
* `AppState` extracted as a plain struct in `dbflux_app`; `AppStateEntity` wrapper with GPUI event emission lives in `dbflux_ui`
* `dbflux_core` reorganized from 50 flat files into 10 thematic subdirectories (`core/`, `driver/`, `schema/`, `sql/`, `query/`, `connection/`, `storage/`, `data/`, `config/`, `facade/`)

### Drivers

* **DynamoDB**: built-in driver with full CRUD, SSM tunnel integration, and AWS SSO auth
* **PostgreSQL, MySQL, SQLite, MongoDB, Redis**: driver stability fixes, schema introspection, filter translation, pagination, and aggregate handling
* Driver crates now include README files documenting features and limitations

### MCP Governance

* Policy engine with roles, trusted clients, and tool policies
* Approval service for deferred destructive/write operations
* SQLite-backed audit logging with CloudWatch-like viewer
* Standalone MCP server (`dbflux mcp`) integrated as optional CLI subcommand
* Granular MCP tools for query, schema, DDL preview, and more

### Connection Infrastructure

* Proxy tunnel support: SOCKS5 and HTTP CONNECT with per-connection selection
* SSH tunnel with adaptive sleep and host key verification
* Connection hooks: reusable Bash/Python/Lua scripts bound to PreConnect, PostConnect, PreDisconnect, PostDisconnect phases
* Unified SQLite storage in `~/.local/share/dbflux/dbflux.db`

### UI/UX

* Tab context menu (Close, Close Others, Close All, Duplicate)
* Settings sidebar with collapsible categories (TreeNav component)
* Audit viewer with full keyboard navigation (`j/k`, `g/G`, `]/[`, `m` for context menu)
* Language-specific script icons in sidebar
* X11 window rendering fixes and platform-aware floating windows
* Live output streaming for script execution

### Security

* `SecretString` end-to-end across core, drivers, and IPC handoff
* Per-process authentication tokens for local IPC and driver RPC
* URI passwords sanitized before persistence
* Lua VM memory capped at 16 MiB

### AWS

* In-app AWS SSO login flow with account/role discovery wizard
* Provider-agnostic auth with runtime-registered `AuthProviderRegistry` (AWS SSO, Static, Shared credentials)
* Managed access via AWS SSM port-forward tunnels (no SSH key needed for RDS/EC2)
* Value sources for managed access fields: SSM Parameter Store, Secrets Manager, environment variables
* SSO auth profiles write back to `~/.aws/config` for compatibility with other AWS tools
* DynamoDB driver uses same managed access pipeline for seamless AWS integration

---

## [0.3.7] – 2026-03-06

### Added

* Dedicated style CI workflow that runs `cargo fmt --check` and `cargo clippy --workspace -- -D warnings`

### Changed

* New connections and SSH tunnels now save credentials to the system keyring by default unless the checkbox is explicitly disabled
* Release workflow now blocks artifact builds until both tests and style checks pass

### Fixed

* SQL query context selectors now refresh when connections and databases change, so tabs opened before connecting can pick their execution target correctly
* Per-database query task cancellation now cleans up the exact connection target, allowing the sidebar to reopen those databases after cancellation
* PostgreSQL connection retry setup and live integration tests now compile cleanly under current type inference requirements
* Restored the full `LICENSE-MIT` text in release artifacts

---

## [0.3.6] – 2026-02-28

### Added

* Comprehensive live integration tests for all five database drivers (43 tests covering schema introspection, CRUD, browse/count, explain, describe, cancellation, code generators, document CRUD, and KeyValueApi)
* Docker-based test infrastructure for PostgreSQL, MySQL, MongoDB, and Redis with automatic container lifecycle management
* Driver contract validation tests for metadata, form definitions, and capability declarations

### Changed

* AppState now accepts an external driver registry, making driver wiring controllable across different runtime contexts
* Document open and query connection selection extracted into explicit decision paths for consistent handling of missing connections and per-database routing
* MySQL `information_schema` queries migrated from `format!()` string interpolation to parameterized queries (`conn.exec` with `?` placeholders)
* MySQL nullable column reads (`Option<String>`) now use `row.get_opt()` to correctly distinguish SQL NULL from missing columns

### Fixed

* MySQL schema introspection panic on MySQL 8.4 where `column_key` in `information_schema.columns` can be NULL
* MySQL constraint introspection panic where `GROUP_CONCAT` over a `LEFT JOIN` returns NULL for CHECK constraints without key columns
* Windows portable builds no longer open a CMD console window when launched outside a terminal
* CI integration test job now installs required system dependencies (`libdbus-1-dev`, `libxkbcommon-dev`)

## [0.3.5] – 2026-02-26

### Added

* Explicit unsupported-value representation in query results (`UNSUPPORTED<type>`) to distinguish decode gaps from real `NULL` values

### Changed

* Unsupported values are now treated as read-only in the data grid and are excluded from save/copy mutation flows

### Fixed

* Added complete PostgreSQL `tsvector`/`tsquery` handling across table browse, query results, and grid filtering
* PostgreSQL fallback decode paths no longer misrepresent unknown types as `NULL`, reducing confusion and avoiding incorrect edits

---

## [0.3.4] – 2026-02-26

### Added

* Inline enum/set dropdown editing in the data grid with keyboard navigation (`j/k`, arrows, `Enter`, `Esc`)
* Nullable enum editing support with explicit `NULL` option in dropdowns
* Driver-level enum value metadata (`enum_values`) in `ColumnInfo` for PostgreSQL and MySQL
* Info-level logging for unsupported value decoding paths in PostgreSQL, MySQL, and SQLite drivers

### Changed

* PostgreSQL column introspection now uses `pg_catalog` + `format_type(...)` to preserve real type names (including user-defined types)
* PostgreSQL generated SQL literals now use escaped single-quoted string literals for readability
* MySQL `ENUM(...)` and `SET(...)` column definitions are parsed and exposed as selectable values in the UI

### Fixed

* PostgreSQL custom types (enum/domain/composite/range) no longer appear as `NULL` due to restrictive string decoding
* Table mode command routing now handles `Execute`/`Cancel` correctly, restoring keyboard-driven inline editing flow
* `LIKE` filter generation now only adds `ESCAPE '\\'` when required by the search value
* PostgreSQL `uuid` columns now cast to `::text` for `LIKE` filters

---

## [0.3.3] – 2026-02-26

### Added

* File-backed "New Tab" flow and keyboard navigation in the context bar
* Settings toggle to mask and reveal SSH password fields
* MongoDB sidebar metadata with collection-level indexes and a database-level indexes folder
* MongoDB field schema sampling in the sidebar (field type, optionality, nested fields)

### Changed

* Sidebar schema folders now stay visible with zero counts while lazy details load
* SQL and MongoDB schema folders are collapsed by default to avoid layout jumps during refresh

### Fixed

* Sidebar expansion no longer gets stuck in a loading state when opening schema nodes
* Closing a database connection no longer blocks the UI thread and freezes the app

---

## [0.3.2] – 2026-02-24

### Added

* Filter submenu in data grid context menu for SQL databases (=, <>, >, <, IS NULL, IS NOT NULL, Remove filter)
* Order submenu in data grid context menu for SQL databases (ASC, DESC, Remove order)
* MongoDB filter submenu in document tree context menu with Extended JSON values, `$and` composition, and NULL semantics (`$exists` guard)
* ListFilter and ArrowUpDown icons
* Empty state for the sidebar connections tab ("No connections yet" hint)

### Changed

* CI release workflow extracts changelog section from CHANGELOG.md instead of using hardcoded text

### Fixed

* GPUI newline panic: escape control characters in `Value::Json` preview in document tree
* GPUI newline panic: escape control characters in `Value::Text` and catch-all rendering in document card view
* GPUI newline panic: use compact JSON (no newlines) when composing MongoDB filters for the single-line filter input
* Toolbar clear-filter button now re-runs the query after clearing (was only calling `cx.notify()` without `refresh()`)
* Refactored icon asset loading to use `ALL_ICONS` lookup table instead of match arms

---

## [0.3.1] – 2026-02-24

### Fixed

* Table expansion in sidebar now loads and displays columns, indexes, and foreign keys instead of showing a stuck "Loading..." placeholder
* Concurrent table expansions no longer overwrite each other (replaced single pending action slot with per-item map)
* Failed schema fetches now collapse the table node instead of leaving it stuck in loading state
* Cache key mismatch between tree builder and fetch path that prevented details from ever appearing for per-database connections

### Added

* Collapsed sidebar now shows separate buttons for Connections and Scripts tabs
* FileCode icon registered in asset source

---

## [0.3.0] – 2026-02-23

### Added

#### MongoDB Support

* MongoDB driver with collection browsing, CRUD operations, and schema introspection
* Document tree view with keyboard navigation, search, and value expansion
* MongoDB query parsing and validation with positional diagnostics
* MongoDB shell query generator for "Copy as Query" support
* Document view context menu with language-aware editor

#### Redis Support

* Redis driver with key-value API integration
* Key-value document browser with keyboard-navigable new-key modal
* Support for all Redis data types: String, Hash, Set, Sorted Set, List, Stream
* Context menu and real pagination for the key browser
* Live TTL countdown display
* Add Member modal for collection types
* Redis key completions and command arity validation in the editor

#### Script Documents

* File-backed query documents with Open (`Ctrl+O`), Save (`Ctrl+S`), and Save As (`Ctrl+Shift+S`)
* Execution context bar with connection, database, and schema dropdowns per tab
* Scripts folder in the sidebar with file and folder management

#### Session Persistence

* Auto-save on a 2-second debounce after each keystroke
* Scratch files for untitled tabs, shadow files for file-backed tabs (explicit `Ctrl+S` still writes the original)
* Full session restore on startup from `~/.local/share/dbflux/sessions/`
* Conflict detection: warns when original file was modified externally while a shadow existed
* Tabs close without unsaved-changes warnings

#### Per-Database Connections

* PostgreSQL supports multiple databases open simultaneously in the sidebar
* Query tabs target a specific database connection instead of sharing a single switchable one

#### Document System

* Tab-based document architecture with `DocumentHandle` and `TabManager`
* SQL query documents with multiple result tabs (MRU ordering)
* Collapsible, resizable sidebar dock and bottom dock panels
* History modal integrated with document-based focus system

#### Editor Enhancements

* Language-aware autocompletion (SQL tables/columns, MongoDB collections, Redis keys)
* Live query diagnostics with positional error markers
* Redis command arity validation in the editor

#### Data Grid

* Inline cell editing with focus handling
* Modal editor for JSON and long text values (`CellEditorModal`)
* Context menu with CRUD operations and SQL generation
* Keyboard navigation in context menus
* Column resizing via drag
* Support for empty tables in the data grid
* Row insert and duplicate without requiring a primary key

#### Query Generation

* Unified query generation with "Copy as Query" and preview modal
* `QueryGenerator` trait implemented by PostgreSQL, MySQL, SQLite, MongoDB, and Redis drivers
* `SqlDialect` trait for SQL flavor differences across drivers

#### Export

* Multi-format export: CSV, JSON, Text, and Binary
* Export generalized by result shape instead of hardcoded CSV

#### Auto-Refresh

* Interval-based auto-refresh with unified refresh split button
* `DocumentTaskRunner` for unified async task tracking

#### Connection Manager

* URI connection mode for PostgreSQL and MySQL
* Bidirectional sync between connection URI and individual form fields

#### Query Safety

* Dangerous query detection for SQL, MongoDB, and Redis commands
* Confirmation dialog with query preview before destructive operations

#### Sidebar

* Schema-level indexes, foreign keys, and data types in the tree
* Schema-level metadata support for MySQL and SQLite
* Context menus for indexes, foreign keys, and custom types
* `q`/`e` keys to switch between Connections and Scripts tabs
* Inline rename in the tree (both tabs)
* Default focus to sidebar on startup when no tabs are open

#### Packaging & CI

* macOS release builds with `.app` bundle (`Info.plist`)
* Windows release builds with Inno Setup installer
* MongoDB and Redis feature flags enabled in default builds

### Changed

* `CellValue` pre-computes display text at construction time (avoids allocation during render)
* Lazy loading for PostgreSQL and SQLite drivers (shallow metadata first, details on demand)
* Sidebar uses `SchemaNodeId` parsing instead of stale underscore prefixes
* Custom toast implementation replaces `gpui-component` toast
* AppState decomposed into focused sub-managers in `dbflux_core`
* Architecture decoupled: core traits, driver capabilities, and error formatting extracted
* Oversized UI modules split into focused submodules (sidebar, SQL query, modals, SSH form)
* Active context detection improved in data grids
* Document focus restored correctly across menus and modals
* Scripts tab styling matches connections tab (icon and label colors)
* Removed force-close flow (double `Ctrl+W` warning, pending force close state)

### Performance

* Fixed catastrophic 1 FPS rendering issue in the data table
* Row-level event handlers replace per-cell closures in tables
* Background executor used consistently for all database operations

### Fixed

* Document focus restored across menus and modals
* Redis database state handling and UI interaction bugs
* SSH tunnel form mouse focus syncs with keyboard state
* Settings sync between SSH form fields
* Panics and unwraps eliminated across UI and driver code
* Empty query results now return column metadata correctly
* DDL queries show preview modal and editor height is correct
* Sidebar "New File"/"New Folder" creates inside the selected folder instead of at root
* Reveal in File Manager works on macOS and Windows (not just Linux)
* Opening an already-open script activates its tab instead of closing it

## [0.2.0] – 2026-01-30

### Added

#### MySQL Support

* MySQL/MariaDB driver with full query execution and schema introspection
* Dual connection architecture (sync for schema, async for queries)
* Dynamic connection forms that adapt to driver-specific requirements

#### Sidebar Enhancements

* Folder-based organization for connection profiles
* Drag and drop support for connections and folders
* Multi-selection (Shift+click, Ctrl+click)
* Keyboard shortcuts for rename, delete, and new folder actions

#### Query Safety

* Confirmation dialogs for dangerous SQL queries (DELETE, DROP, TRUNCATE without WHERE)
* Driver-delegated SQL generation from context menu (SELECT, INSERT, UPDATE, DELETE)

#### Results Table

* Column sorting via header clicks (ASC/DESC)
* Custom DataTable component with virtualized rendering

#### Icons

* Centralized SVG icon system with `AppIcon` enum and compile-time embedding
* Icons across the editor toolbar (History, Save), Run/Cancel buttons, and tabs
* Sidebar tree icons (database brands, folders, tables, views, columns, indexes)
* Icons in context menus, results footer, pagination, and export actions
* Icons in connection manager (tabs, form headers, buttons)
* Icons in settings sidebar and About section
* Icons in toast notifications (success, info, warning, error)
* Icons in confirmation dialogs (delete, dangerous query)
* Database brand icons for PostgreSQL, MySQL, MariaDB, and SQLite
* Third-party licenses listed in About (Lucide ISC, Simple Icons CC0)

#### Packaging & Distribution

* Nix flake with development shell
* Arch Linux PKGBUILD
* Linux installer script (`curl | bash`)
* GPG-signed release artifacts
* GitHub Actions–based release workflow

### Changed

* Lazy loading of table details in the sidebar (improves performance on large schemas)
* Schema loading deferred until node expansion
* Active databases are now visually highlighted in the sidebar

### Performance

* Eliminated hover-induced re-renders in the data table
* Fixed subscription leaks in the table component

### Fixed

* Horizontal auto-scroll when navigating the data table with the keyboard

## [0.1.2] - 2025-01-25

### Fixed

- Connection Manager: SQLite form navigation now works correctly (`j/k` navigates between Name, File Path, and action buttons instead of jumping to non-existent PostgreSQL fields)
- Connection Manager: Pressing Enter while editing an input now exits edit mode and moves to the next field
- Connection Manager: Input blur events now properly restore keyboard navigation focus

## [0.1.1] - 2025-01-25

### Added

- About section in Settings with version info, GitHub links, and license (Apache 2.0 / MIT)
- SSH tunnel form keyboard navigation (row-based: `j/k` between rows, `h/l` within fields, `Tab` sequential, `g/G` first/last)
- Database switch now appears as cancellable background task

### Fixed

- Settings window now opens as singleton (reuses existing window instead of opening duplicates)
- Stale settings window handle is now cleared when the window is closed
- SSH form field selection resets to valid field when switching auth method (PrivateKey ↔ Password)
- SSH selected index adjusts correctly when tunnels are deleted
- `z` keybinding for panel collapse now works in Editor and Background Tasks (previously only Results)

## [0.1.0] - 2025-01-25

Initial release of DBFlux.

### Added

#### Database Support
- PostgreSQL driver with full query execution and schema introspection
- SQLite driver for local database files
- SSL/TLS support for PostgreSQL (Disable, Prefer, Require modes)
- SSH tunnel support with multiple authentication methods (key, password, agent)
- Reusable SSH tunnel profiles

#### User Interface
- Three-panel workspace layout (Sidebar, Editor, Results)
- Resizable and collapsible panels
- Schema tree browser with hierarchical navigation (databases, schemas, tables, views, columns, indexes)
- Visual indicators for column properties (primary key, nullable, type)
- Multi-tab SQL editor with syntax highlighting
- Virtualized results table with column resizing
- Table browser mode with WHERE filters, custom LIMIT, and pagination
- Command palette with fuzzy search and scroll support
- Toast notifications for user feedback
- Background tasks panel with progress and cancellation
- Status bar showing connection and task status
- Keyboard-navigable context menus with nested submenu support

#### SQL Execution
- Query execution with result display
- Query cancellation support (PostgreSQL uses `pg_cancel_backend`, SQLite uses `sqlite3_interrupt`)
- Execution time and row count display
- Multiple result tabs

#### Query Management
- Query history with timestamps and execution metadata
- Saved queries with favorites support
- Search and filter across history and saved queries
- Unified history/saved queries modal with keyboard navigation
- Persistent storage in `~/.config/dbflux/`

#### Connection Management
- Connection profiles with secure password storage (system keyring)
- Connection manager with full form validation
- Test connection before saving
- Quick connect/disconnect from sidebar

#### Keyboard Navigation
- Vim-style navigation (j/k/h/l) throughout the application
- Context-aware keybindings (Sidebar, Editor, Results, History, Settings)
- Global shortcuts for common actions
- Tab cycling between panels
- Full keyboard support in connection manager form
- Results toolbar navigation: `f` to focus toolbar, `h/l` to navigate elements, `Enter` to edit/execute, `Esc` to exit
- Panel collapse toggle with `z` key
- Context menu navigation: `j/k` to move, `Enter` to select, `l` to open submenu, `h/Esc` to close

#### Export
- CSV export for query results

#### Settings
- SSH tunnel profile management
- Keybindings reference section with collapsible context groups and search filter

### Known Limitations

- No dark/light theme toggle (uses system default)
