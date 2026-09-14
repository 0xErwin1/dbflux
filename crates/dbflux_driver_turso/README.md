# TursoDB

Remote Turso / libSQL database over HTTP.

## At a glance

- **Category** — Relational
- **Query language** — SQL (SQLite dialect)
- **Default port** — none; the endpoint is a URL
- **URI scheme** — `libsql` (also `turso`, `https`, and `http` for a local `sqld`)

## Connecting

| Field | Notes |
|---|---|
| URL | `libsql://<database>-<org>.turso.io`, or `http://127.0.0.1:8080` for a local `sqld`. Credentials, query strings, fragments, and paths are rejected. |
| Auth Token | Held in the OS keyring and sent as a bearer token. Optional for unauthenticated local servers. |

## Features

- Hrana-over-HTTP transport through the `turso_serverless` SDK, driven by a
  per-connection Tokio runtime so the synchronous driver contract is preserved.
- Arbitrary SQL with bound parameters; multi-statement scripts run as one
  pipelined batch that stops at the first failing statement.
- Schema discovery from `sqlite_master` and the `table_info`, `index_list`,
  `index_info`, and `foreign_key_list` PRAGMAs: tables, views, columns,
  indexes, foreign keys, and CHECK/UNIQUE constraints.
- Typed `INSERT`, `UPDATE`, and `DELETE` through the shared SQL query builder,
  with the affected row re-read so the grid shows server-side values.
- Interactive transactions: each editor document runs on its own server stream
  through the generic execution-session seam, so `BEGIN` … `COMMIT` spans
  several runs while grid edits and MCP calls use isolated streams and never
  join it.
- Column kinds from declared types with SQLite affinity rules, falling back to a
  scan of the returned values for expression columns.
- Pagination, sorting, and filtering as SQL `LIMIT`/`OFFSET`, `ORDER BY`, and
  `WHERE`, plus CSV and JSON export.
- SQLite-style code generation (`CREATE TABLE` with rowid semantics,
  `ADD COLUMN`, `DROP COLUMN`, indexes, `REINDEX`) and the visual query builder.
- Dangerous-query detection uses the shared `SqlLanguageService`: `DELETE` and
  `UPDATE` without `WHERE`, `DROP`, `TRUNCATE`, and `ALTER` are flagged as for
  any other SQL driver.
- Error classification for auth failures, constraint violations, syntax errors,
  missing objects, and busy servers, without leaking response bodies.

## Limitations

- No query cancellation. A request runs until the server answers or the SDK's
  transport gives up.
- No SSH tunnels, embedded replicas, local files, or sync; local `sqld`
  instances are reached over plain HTTP only.
- In the editor, transaction control is one statement per run: `BEGIN`,
  `COMMIT`, and `ROLLBACK` each run on their own, and a script that mixes
  them with other statements is rejected before reaching the server.
  `SAVEPOINT`, `RELEASE`, and `ROLLBACK TO` are rejected the same way.
- One database per connection; `ATTACH` and database switching are not exposed.
- No write-privilege probe: the mutation policy is not tightened for
  read-only tokens, so a write with a read-only token fails at execution time.
- No instance metrics, inspectors, or dashboard sources.
- Data transfer into Turso is untested; the driver advertises bulk insert but
  the transfer engine has not been exercised against a Turso endpoint.
- Results are fully buffered by the SDK before rows are returned.
- `PRAGMA foreign_keys` toggling is not exposed because it is per-stream and
  would not apply to isolated sessions.
- Request timeouts are the SDK defaults and are not configurable yet.
- Live tests are gated behind `TURSO_TEST_URL` / `TURSO_TEST_TOKEN` and run
  only with `--ignored`.
