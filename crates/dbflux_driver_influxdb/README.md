# dbflux_driver_influxdb

InfluxDB driver for DBFlux.

## Features

- **InfluxDB v1 and v2** — both API versions are supported in a single driver crate.
- **InfluxQL on both versions** — the v1 query language works on v1 and via the v2 compatibility endpoint.
- **Flux on v2** — Flux queries are available when the connection is configured for v2.
- **Automatic time-window injection** — when a time range is set via the source context panel and the query does not already contain a time predicate (`time >=` etc. for InfluxQL, `|> range(` for Flux), the driver injects the bounds automatically.
- **Structured error messages** — server-side errors are parsed from the JSON `{"error": "..."}` field instead of being displayed as raw HTTP status codes.
- **CSV and JSON export** — query results can be exported through the standard DBFlux export pipeline.
- **Audit emission** — all queries are tracked through the standard DBFlux audit sink.

## Limitations

- **Flux not supported on v1** — attempting to run a Flux query against a v1 connection returns an error immediately, without making an HTTP call.
- **No INSERT/UPDATE/DELETE** — InfluxDB's query API is read-only. Data ingestion uses the Line Protocol write API which is not exposed by this driver.
- **No transactions** — InfluxDB does not support transactions.
- **Regex-based time predicate detection** — the driver uses regular expressions to determine whether a query already contains a time predicate. This may false-positive on quoted string literals that happen to contain text matching `time <`, `time >`, or `|> range(`.
- **Multi-statement InfluxQL returns first result only** — when a query contains multiple statements separated by `;`, only the result of the first statement is returned. Remaining results are discarded.
- **Basic auth via Authorization header** — v1 username/password credentials are sent as an `Authorization: Basic <base64>` header rather than via URL query parameters. This is cleaner for log hygiene but differs from some InfluxDB client libraries.
