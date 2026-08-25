# ClickHouse

Column-oriented analytical database over HTTP.

## At a glance

- **Category** — Relational
- **Query language** — SQL
- **Default port** — 8123
- **URI scheme** — `http`

## Connecting

The driver speaks ClickHouse's HTTP interface rather than the native protocol, so
the endpoint is a URL and not a host/port pair.

| Field | Default | Notes |
|---|---|---|
| HTTP URL | `http://localhost:8123` | `https://` endpoints are served over rustls |
| Database | `default` | Scopes schema discovery and unqualified queries |
| Request timeout | `30` seconds | Must be greater than zero |
| User | `default` | |
| Password | — | Held in the OS keyring and sent as HTTP Basic auth |

## Features

- Blocking HTTP(S) transport built on rustls, authenticating with HTTP Basic.
- Arbitrary single-statement SQL, with responses forced to `JSONCompact` so column
  names and types arrive alongside the rows.
- Row width is checked against the declared column count on every response, so a
  malformed reply fails with a clear error instead of shifting values between columns.
- Schema discovery from `system.databases`, `system.tables` and `system.columns`:
  databases, tables, views, columns, engine, sorting and partition keys, on-disk
  size, and compression.
- Schema loading is lazy per database, so a server with many databases does not pay
  for all of them at connect time.
- Pagination, sorting and filtering are applied by the driver as `LIMIT`/`OFFSET`
  around the statement, which is what makes result browsing work without a cursor.
- Read-only visual SELECT generation, using ClickHouse identifier and literal
  quoting rules.
- Chart authoring from query results, and CSV and JSON export.

### Type handling

Values are decoded recursively, so a `Map(String, Array(Nullable(Decimal256)))`
arrives fully structured rather than as raw text:

- Wrappers — `Nullable`, `LowCardinality`
- Containers — `Array`, `Tuple`, `Map`, `Nested`
- Numbers — integers up to `UInt256`, `Decimal256`, `BFloat16`, `Bool`
- Time — `Date32`, `DateTime64`
- Other — `Enum16`, `Nothing`

## Limitations

- No SSH tunnels.
- No transactions, prepared statements, or query cancellation. A running statement
  is bounded by the request timeout and nothing else, and the driver reports no
  lock-timeout support.
- No structured `INSERT`, `UPDATE`, `DELETE`, DDL, or data-transfer support. Write
  SQL runs only when typed explicitly into the editor, so the grid is read-only and
  the driver is not a transfer target.
- One SQL statement per request; multi-statement scripts are not batched.
- HTTP response bodies are capped at 128 MiB.
- Named ClickHouse time zones are not interpreted client-side. ISO timestamps
  carrying an offset are handled accurately; timestamps without one are treated
  as UTC.
