# dbflux_driver_influxdb

InfluxDB driver for DBFlux.

## Features

- **Time-series category** — classified as `DatabaseCategory::TimeSeries` with `QueryLanguage::InfluxQuery` as the default editor language. Declared capabilities are `AUTHENTICATION`, `MULTIPLE_DATABASES`, `PAGINATION`, `EXPORT_CSV`, and `EXPORT_JSON`. Connections use the `http` URI scheme on default port 8086, with TLS provided by the rustls-backed HTTP client.
- **InfluxDB v1 and v2** — both API versions are supported in a single driver crate.
- **InfluxQL on both versions** — the v1 query language works on v1 and via the v2 compatibility endpoint.
- **Flux on v2** — Flux queries are available when the connection is configured for v2.
- **Optional default bucket** — the connection profile's bucket (v2) or database (v1) field is optional. A v2 API token gives access to all buckets in the organisation; a v1 user gives access to all databases on the server. Leaving the field blank lets the user select a bucket per-query from the source-context dropdown in the editor. Setting it pre-selects that bucket without restricting access to others.
- **Per-query bucket routing** — the bucket used for each InfluxQL query comes from the source-context dropdown selection, not the connection profile. For Flux queries the bucket is embedded in the query text itself (`from(bucket: "...")`).
- **Bucket-free ping** — the connection liveness check does not require a bucket: v1 uses `SHOW DATABASES` against the internal database; v2 fetches `/api/v2/buckets?limit=1`.
- **Time range macros** — InfluxQL and Flux queries support Grafana-compatible macro tokens that are substituted with the bound time-range window before the query is sent to the driver:

  | Token | Language | Expansion |
  |---|---|---|
  | `$timeFilter` | InfluxQL | `time >= 'RFC3339_start' AND time <= 'RFC3339_end'` |
  | `$__from` | InfluxQL | `'RFC3339_start'` |
  | `$__to` | InfluxQL | `'RFC3339_end'` |
  | `v.timeRangeStart` | Flux | `'RFC3339_start'` |
  | `v.timeRangeStop` | Flux | `'RFC3339_end'` |

  These tokens match Grafana's variable conventions (`$timeFilter` for InfluxQL, `v.timeRangeStart`/`v.timeRangeStop` for Flux). Users familiar with Grafana should find the syntax intuitive.

  RFC3339 format: `YYYY-MM-DDTHH:MM:SSZ` (UTC, second precision, Z suffix).

  **InfluxQL example** — using `$timeFilter`:

  ```influxql
  -- Typed:
  SELECT mean(usage_user) FROM cpu WHERE $timeFilter GROUP BY time(1m)

  -- Executed (window = 2026-05-20T00:00:00Z to 2026-05-22T23:59:00Z):
  SELECT mean(usage_user) FROM cpu WHERE time >= '2026-05-20T00:00:00Z' AND time <= '2026-05-22T23:59:00Z' GROUP BY time(1m)
  ```

  **Flux example** — using `v.timeRangeStart` / `v.timeRangeStop`:

  ```flux
  -- Typed:
  from(bucket: "telegraf")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "cpu")

  -- Executed (same window):
  from(bucket: "telegraf")
    |> range(start: '2026-05-20T00:00:00Z', stop: '2026-05-22T23:59:00Z')
    |> filter(fn: (r) => r._measurement == "cpu")
  ```

  **Macros require a bound window** — if the query contains macro tokens but no time-range window is set (i.e., the source-context panel has no selection), the macros pass through to the driver unsubstituted. InfluxDB will return a parse error since `$timeFilter` etc. are not valid InfluxQL/Flux syntax.

  **Macros suppress inject-when-absent** — when a query contains any of the recognized macro tokens, the automatic time-window injection (see below) is suppressed. The macro substitution is treated as the user's authoritative time bound.

  **v1 known limitation (naïve substring substitution)** — macro tokens inside quoted string literals or comments are also substituted. There is no escape syntax in v1. For Flux, a variable whose name merely starts with `v.timeRangeStart` or `v.timeRangeStop` (e.g. `v.timeRangeStartCustom`) will also be substituted. Proper tokenisation is planned for a future version.

- **Automatic time-window injection** — when a time range is set via the source context panel and the query does not already contain a time predicate (`time >=` etc. for InfluxQL, `|> range(` for Flux), the driver injects the bounds automatically. This behavior is suppressed when the query contains explicit time-range macro tokens.
- **Structured error messages** — server-side errors are parsed from the JSON `{"error": "..."}` field instead of being displayed as raw HTTP status codes.
- **CSV and JSON export** — query results can be exported through the standard DBFlux export pipeline.
- **Audit emission** — all queries are tracked through the standard DBFlux audit sink. The `bucket_or_database` metadata field records the actual bucket used for each query, not the profile default.
- **Multi-statement InfluxQL** — when a query contains multiple statements separated by `;` (e.g. `SHOW MEASUREMENTS; SHOW SERIES`), all results are concatenated into a single result set. A synthetic `statement_index` integer column is prepended to distinguish rows from different statements.
- **"Query Measurement" context menu** — right-clicking a measurement in the sidebar shows "Query Measurement". The action opens a new code document pre-populated with a template query (`SELECT * FROM ...` for InfluxQL, `from(bucket: ...) |> range(...)` for Flux).
- **"New Query" context menu on buckets** — right-clicking a bucket/database node shows "New Query", opening a blank code document with the connection activated.
- **Read-template generation** — `InfluxQueryGenerator` produces select-all and per-measurement read templates for both InfluxQL and Flux (used by the context-menu actions and copy-as-query), version-aware via the connection's configured version and default bucket.

## Limitations

- **No query cancellation** — `cancel()` returns `NotSupported`; in-flight queries cannot be aborted from the UI (`QUERY_CANCELLATION` is not declared).
- **No mutation generation** — `QueryGenerator::generate_mutation` always returns `None`; only read templates are generated, consistent with the read-only query API.
- **Flux not supported on v1** — attempting to run a Flux query against a v1 connection returns an error immediately, without making an HTTP call.
- **No INSERT/UPDATE/DELETE** — InfluxDB's query API is read-only. Data ingestion uses the Line Protocol write API which is not exposed by this driver.
- **No transactions** — InfluxDB does not support transactions.
- **InfluxQL requires a bucket** — InfluxQL queries embed the bucket in the URL (`?db=<bucket>`). If neither the source-context dropdown nor the profile default provides a bucket, execution is rejected with a clear error asking the user to select one.
- **Regex-based time predicate detection** — the driver uses regular expressions to determine whether a query already contains a time predicate. This may false-positive on quoted string literals that happen to contain text matching `time <`, `time >`, or `|> range(`.
- **Multi-statement columns are fixed by the first non-empty statement** — when a multi-statement query returns results with different shapes (e.g. `SHOW MEASUREMENTS; SHOW SERIES`), the column layout is determined by the first non-empty statement. Rows from subsequent statements are mapped to that layout. Mismatched shapes produce misaligned columns rather than an error.
- **Basic auth via Authorization header** — v1 username/password credentials are sent as an `Authorization: Basic <base64>` header rather than via URL query parameters. This is cleaner for log hygiene but differs from some InfluxDB client libraries.
- **Backwards-compatible serialisation** — profiles saved with the old required `bucket_or_database` field continue to load correctly. The field is deserialized as `default_bucket` via a serde alias. Profiles saved after this change use the `default_bucket` key.
