# dbflux_driver_influxdb

InfluxDB driver for DBFlux.

## Features

- **InfluxDB v1 and v2** — both API versions are supported in a single driver crate.
- **InfluxQL on both versions** — the v1 query language works on v1 and via the v2 compatibility endpoint.
- **Flux on v2** — Flux queries are available when the connection is configured for v2.
- **Optional default bucket** — the connection profile's bucket (v2) or database (v1) field is optional. A v2 API token gives access to all buckets in the organisation; a v1 user gives access to all databases on the server. Leaving the field blank lets the user select a bucket per-query from the source-context dropdown in the editor. Setting it pre-selects that bucket without restricting access to others.
- **Per-query bucket routing** — the bucket used for each InfluxQL query comes from the source-context dropdown selection, not the connection profile. For Flux queries the bucket is embedded in the query text itself (`from(bucket: "...")`).
- **Bucket-free ping** — the connection liveness check does not require a bucket: v1 uses `SHOW DATABASES` against the internal database; v2 fetches `/api/v2/buckets?limit=1`.
- **Automatic time-window injection** — when a time range is set via the source context panel and the query does not already contain a time predicate (`time >=` etc. for InfluxQL, `|> range(` for Flux), the driver injects the bounds automatically.
- **Structured error messages** — server-side errors are parsed from the JSON `{"error": "..."}` field instead of being displayed as raw HTTP status codes.
- **CSV and JSON export** — query results can be exported through the standard DBFlux export pipeline.
- **Audit emission** — all queries are tracked through the standard DBFlux audit sink. The `bucket_or_database` metadata field records the actual bucket used for each query, not the profile default.

## Limitations

- **Flux not supported on v1** — attempting to run a Flux query against a v1 connection returns an error immediately, without making an HTTP call.
- **No INSERT/UPDATE/DELETE** — InfluxDB's query API is read-only. Data ingestion uses the Line Protocol write API which is not exposed by this driver.
- **No transactions** — InfluxDB does not support transactions.
- **InfluxQL requires a bucket** — InfluxQL queries embed the bucket in the URL (`?db=<bucket>`). If neither the source-context dropdown nor the profile default provides a bucket, execution is rejected with a clear error asking the user to select one.
- **Regex-based time predicate detection** — the driver uses regular expressions to determine whether a query already contains a time predicate. This may false-positive on quoted string literals that happen to contain text matching `time <`, `time >`, or `|> range(`.
- **Multi-statement InfluxQL returns first result only** — when a query contains multiple statements separated by `;`, only the result of the first statement is returned. Remaining results are discarded.
- **Basic auth via Authorization header** — v1 username/password credentials are sent as an `Authorization: Basic <base64>` header rather than via URL query parameters. This is cleaner for log hygiene but differs from some InfluxDB client libraries.
- **Backwards-compatible serialisation** — profiles saved with the old required `bucket_or_database` field continue to load correctly. The field is deserialized as `default_bucket` via a serde alias. Profiles saved after this change use the `default_bucket` key.
