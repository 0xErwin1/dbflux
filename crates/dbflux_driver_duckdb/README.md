# DuckDB

Embedded analytical database, with optional DuckLake catalogs.

## At a glance

- **Category** — Relational
- **Query language** — SQL (DuckDB dialect)
- **Default port** — none; the database is a local file or in memory
- **URI scheme** — `duckdb`

## Connecting

| Field | Notes |
|---|---|
| Database File | Path to a `.duckdb` file, created if missing. Leave empty for an in-memory database that lives as long as the profile has an open connection. |
| Catalog | Optional DuckLake catalog, for example `metadata.ducklake` or `postgres:dbname=lake`. It is attached as `lake` and becomes the default database. Stored in the profile in plain text, so keep passwords out of a DSN and use a DuckDB secret from Init SQL instead. |
| Data Path | Optional DuckLake `DATA_PATH`, for example `s3://bucket/lake/`. It is passed with `OVERRIDE_DATA_PATH true`, so it replaces the path stored in the catalog for this connection. |
| Init SQL | Optional statements run once when the database is opened, before the DuckLake attach: `INSTALL httpfs;`, `CREATE SECRET (TYPE s3, PROVIDER credential_chain);`, `SET` options. |

## Features

- Bundled DuckDB engine with the Parquet and JSON extensions built in; other
  extensions (`ducklake`, `httpfs`, `postgres`, ...) install on demand.
- DuckLake: the catalog is attached with `ATTACH IF NOT EXISTS 'ducklake:...'`
  on connect and selected with `USE lake`.
- Every attached catalog is listed as a database in the sidebar; opening one
  runs `USE`, and its schemas, tables, and views load lazily from
  `duckdb_tables()` and `duckdb_views()`.
- Table details from `duckdb_columns()`, `duckdb_constraints()`, and
  `duckdb_indexes()`: column types, nullability, defaults, the primary key,
  foreign keys, CHECK and UNIQUE constraints, and indexes. The schema-level
  Indexes and Foreign Keys folders come from the same catalog functions.
- Multi-statement scripts; the result grid shows the last statement's rows.
  Bound parameters, request row limits, and query cancellation through
  DuckDB's interrupt handle.
- DuckDB types map to grid values: integers up to `HUGEINT` (values past
  `i64` render as decimals), `DECIMAL`, timestamps, dates, times, `BLOB`,
  `LIST`/`ARRAY`, `STRUCT`, `MAP`, `UNION`, and `ENUM`.
- Typed `INSERT` and `DELETE` from the grid using `RETURNING *`, and `UPDATE`
  followed by a read of the row by its key, because DuckDB refuses
  `UPDATE ... RETURNING` on a row that a foreign key references. The shared
  SQL mutation generator serves previews and the visual query builder.
- Within the app, connections to the same file share one DuckDB instance, so
  several tabs, the sidebar, and the built-in MCP server can use one file
  without tripping DuckDB's file lock. Relative and absolute paths to one file
  share the instance.
- Each editor tab, grid mutation, and MCP call runs in its own session (a
  separate DuckDB connection to the shared instance), so a transaction left
  open in the editor does not capture other requests.

## Limitations

- Read-only enforcement (MCP reads, editor auto-refresh) accepts SELECT
  statements only, checked with DuckDB's own parser, and runs them in a
  `READ ONLY` transaction. Other statements, and requests inside an open
  transaction, are refused with `NotSupported`.
- A `READ ONLY` transaction still lets a SELECT read files and URLs, so
  read-only requests also refuse table functions other than `range`,
  `generate_series`, `unnest`, `json_each`, `json_tree`, and the `duckdb_*`
  (except `duckdb_secrets`), `pragma_*`, and `ducklake_*` functions, and any
  table name containing `.`, `/`, `\`, or `:`, which DuckDB could read as a file.
  A table whose name contains one of those characters cannot be read through
  MCP. Only the statement itself is checked: a view or macro created earlier
  can still read files when a read-only request selects from it, and scalar
  functions added by extensions are not checked.
- Statement timeouts are refused with `NotSupported`.
- Only one process can open a database file for writing; DBFlux cannot open a
  file that another program (the DuckDB CLI, a notebook, or the standalone
  `dbflux mcp` server) holds open.
- A file that is already open can only be joined by a profile with the same
  DuckLake and Init SQL settings, because those apply to the whole instance.
  A profile with other settings is refused until the other connections close.
- DuckDB returns `TIMESTAMP` and `TIMESTAMPTZ` values alike as UTC instants.
  Edits use the column type to write the right literal, but a filter or row
  identity without a column type is written as `TIMESTAMP`, which can shift a
  `TIMESTAMPTZ` comparison by the session time zone once the ICU extension is
  loaded.
- `INTERVAL` values are shown as text in DuckDB's notation, such as
  `1 month 2 days 00:01:30`.
- Result columns are typed from their storage type, so `JSON` and `UUID`
  columns show as `VARCHAR` and `HUGEINT` as `DECIMAL(38,0)`.
- Grid queries run against the active catalog, so open a catalog in the sidebar
  before browsing its tables. Generated SQL names a table as `schema.table`, so
  two tabs on different catalogs of one connection share the single active
  catalog, and the one opened last wins.
- `Init SQL` and the DuckLake `Catalog`, which may be a DSN such as
  `postgres:dbname=lake password=...`, are stored in the connection profile in
  plain text. Keep credentials
  out of it and use `CREATE SECRET ... (PROVIDER credential_chain)` or
  environment-based providers instead.
- Extensions that are not bundled are downloaded from the DuckDB extension
  repository the first time they load, which needs network access.
