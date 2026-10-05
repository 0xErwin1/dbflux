# PostgreSQL

Advanced open-source relational database.

## At a glance

- **Category** — Relational
- **Query language** — SQL
- **Default port** — 5432
- **URI scheme** — `postgresql`

## Features

- PostgreSQL relational driver with SQL query execution and schema discovery.
- Native console (`NATIVE_CONSOLE`): tables dock a console, and a database's sidebar menu opens one in its own tab, that runs one SQL statement line at a time through `Connection::execute` with the editor's validation, dangerous-query confirmation, audit rows and query history. The driver enforces `QueryRequest::limit` (`REQUEST_ROW_LIMIT`), so console commands carry the editor row limit.
- Supports schemas, tables, views, indexes, foreign keys, check constraints, unique constraints, and custom types.
- Exposes stored routines (functions, procedures, aggregates, window functions) in the schema tree with read-only definition viewer.
- Supports authentication, SSL, SSH tunneling, and URI/manual connection modes.
- Supports query cancellation through PostgreSQL cancel tokens.
- Includes PostgreSQL-specific SQL/code generation for CRUD, indexes, reindex, foreign keys, and type operations.
- Multi-statement scripts (several `;`-separated statements) run as a batch via the simple query protocol, returning one result set per statement.
- Data-transfer engine: native multi-row `INSERT` bulk-load (`BULK_INSERT`), driver-native `CREATE TABLE` DDL from a source table's columns, `TRUNCATE TABLE` support, and a referential-integrity toggle (`SET session_replication_role`) for FK-safe migrations.
- Displays `pgvector` `vector`, `halfvec`, and `sparsevec` values, including verified one-dimensional arrays, as textual results.
- Displays full-text search `tsvector` and `tsquery` values, including one-dimensional arrays, in PostgreSQL's canonical text form.
- Displays `numeric` values, including one-dimensional `numeric[]` arrays, as the exact decimal PostgreSQL stores.
- Displays `infinity` and `-infinity` dates, timestamps, and their one-dimensional arrays as PostgreSQL's own text.
- Reports its client identity to the server as `application_name=dbflux/<version>` unless the connection string already sets `application_name`, in which case the user-supplied value is kept.
- Probes write privilege after connecting: a replica or a read-only transaction mode resolves to read-only regardless of grants, otherwise the authenticated role's `INSERT`/`UPDATE`/`DELETE` privileges on visible base tables decide it; an empty database or a probe failure is inconclusive and leaves the profile's own mutation policy unchanged.

### Instance Metrics

Exposes a curated set of live server metrics sourced from PostgreSQL system views:

- `pg.tps` — transactions per second (from `pg_stat_database`)
- `pg.cache_hit_ratio` — buffer cache hit ratio (from `pg_statio_user_tables`)
- `pg.active_connections` — connections in state `'active'`
- `pg.idle_connections` — connections in state `'idle'`
- `pg.blocks_read` — blocks read from disk (from `pg_statio_user_tables`)
- `pg.stat_statements.mean_exec_ms` — mean execution time per query (requires `pg_stat_statements` extension)

Each metric is returned as a single `(timestamp_ms, value)` row for live charting.

### Instance Inspector

Exposes tabular snapshots of running server state:

- `pg.activity` — current sessions from `pg_stat_activity` (query text, state, wait event, duration)
- `pg.locks` — active locks from `pg_locks` joined with `pg_class`

- Row limits stream results, retain at most the requested count across the whole request, and report for each result set whether it omitted rows; execution always drains to completion.
- A row-limited multi-statement batch runs statement by statement through the same typed streaming path, under one row budget shared by the whole request. Statements after the budget is exhausted still run, result sets keep the unbounded order (the first is the primary result), and the batch stops at the first failure.
- A row-limited batch keeps the transaction boundaries the same script has when it runs unbounded as one simple query. Statements outside a user transaction run inside a transaction the driver opens and commits, or rolls back on failure, so a failure leaves none of them behind. A `BEGIN` in the script adopts the statements before it and a `COMMIT` or `ROLLBACK` ends it, as PostgreSQL's implicit transaction block does. Inside an open session transaction the statements join it, and a failure leaves it aborted.
- Requested statement timeouts are rejected before execution.

## Limitations

- Row limits cap retained rows, not server work, network traffic, or execution time; mutations still complete all effects.
- Row limits on instance metrics and inspectors are rejected before dispatch. Unbounded batches retain their buffered behavior.
- A row-limited batch refuses, before any statement runs, `PREPARE TRANSACTION`, and a `SAVEPOINT`, `RELEASE`, `ROLLBACK TO`, or chained `COMMIT`/`ROLLBACK` that follows statements outside an explicit transaction. PostgreSQL rejects those inside its implicit transaction block, while the transaction the driver opens would accept them.
- Statements that cannot run inside a transaction block, such as `VACUUM` or `CREATE INDEX CONCURRENTLY`, fail inside a row-limited batch with PostgreSQL's own error, as they do in an unbounded batch.
- A row-limited batch is split with the SQL editor's statement splitter, which does not recognise SQL-standard `BEGIN ATOMIC` function bodies. A batch that contains one fails with a syntax error and rolls back instead of running; the same function alone in a request runs normally.


- Unbounded batched (multi-statement) result columns carry no type metadata; values are returned as text and chart auto-detection is disabled for them. Run a single statement to get fully typed columns.

- `pg.stat_statements.mean_exec_ms` is only available when the `pg_stat_statements` extension is installed and loaded. The driver probes for its presence at catalog construction time; when absent the metric is omitted from `list_metrics()`.

- Instance metrics return a single data point per call (current snapshot), not a historical time series. The UI polls at the configured refresh interval to build the live chart.

- SQL-only driver; it does not expose document or key-value APIs.
- A non-NULL single-statement value the driver cannot decode, such as a value of a type it has no decoder for or a date outside the range it can represent, is shown as an unsupported type and flagged in the result; a real `NULL` still shows as `NULL`. Cast the column to `text` to read the server's own text. Instance inspectors log the column and type of such a cell.
- `money` values are shown as an unsupported type: the wire format carries an integer amount whose decimal scale comes from the server's `lc_monetary` setting, which the client cannot see. Cast the column to `numeric` or `text` to read it.
- Routine definitions for aggregate and window functions are synthesized from catalog metadata because `pg_get_functiondef` does not support them.
- Routine editing and execution are not supported; the routine viewer is read-only.
- Cancellation is best effort and depends on server/session state at cancellation time.
- Code generation targets supported PostgreSQL constructs only; unsupported generator IDs return `NotSupported`.
- Read-only enforcement: a request DBFlux runs unattended as a read (MCP `execute_script` scripts classified `Read` or `Metadata`, editor auto-refresh) runs in `BEGIN READ ONLY` and is rolled back, and is refused inside an open transaction. PostgreSQL then rejects data modification in the session, but not functions with external effects such as `dblink_exec`, `COPY ... TO PROGRAM`, `lo_export`, `pg_terminate_backend` or advisory locks; least-privilege database credentials remain the real boundary.

## DDL Capabilities

### Transactional DDL

PostgreSQL supports **transactional DDL** — all DDL operations (except `CREATE INDEX CONCURRENTLY`) can be wrapped in transactions and rolled back:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;
-- Test the change
ROLLBACK;  -- Safe to rollback if something goes wrong
```

**Exception**: `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` cannot run inside a transaction.

### ALTER TABLE Behavior

**Adding columns with defaults (PostgreSQL 11+)**:
- Fast (metadata-only operation)
- No table rewrite required
- Does not lock table for reads/writes

**Adding columns without defaults**:
- Fast (no rewrite)
- Existing rows get `NULL` for new column

**Changing column types**:
- May require table rewrite (locks table)
- Use `USING` clause for custom conversion: `ALTER COLUMN age TYPE integer USING age::integer`

**Dropping columns**:
- Fast (marks column as dropped, no rewrite)
- Data is not immediately reclaimed (use `VACUUM FULL` if needed)

**Renaming columns**:
- Fast (metadata-only)
- May break views, triggers, and application code

### Index Operations

**CREATE INDEX**:
- Locks table for writes (reads allowed)
- Use `CONCURRENTLY` for zero-downtime index creation:
  ```sql
  CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
  ```

**DROP INDEX**:
- Locks table for writes (reads allowed)
- Use `CONCURRENTLY` for zero-downtime index removal:
  ```sql
  DROP INDEX CONCURRENTLY idx_users_email;
  ```

**REINDEX**:
- Locks table for reads and writes
- Use `CONCURRENTLY` (PostgreSQL 12+) for zero-downtime reindex

### Constraints

**Adding constraints**:
- `CHECK` and `UNIQUE` constraints scan table (may take time on large tables)
- Use `NOT VALID` to defer validation:
  ```sql
  ALTER TABLE users ADD CONSTRAINT age_check CHECK (age >= 0) NOT VALID;
  -- Later, validate without locking:
  ALTER TABLE users VALIDATE CONSTRAINT age_check;
  ```

**Foreign keys**:
- Adding foreign keys scans both tables
- Use `NOT VALID` + `VALIDATE CONSTRAINT` for zero-downtime FK creation

### Custom Types

**CREATE TYPE (enum)**:
- Fast (metadata-only)
- Use `ALTER TYPE ... ADD VALUE` to add enum values:
  ```sql
  ALTER TYPE status_enum ADD VALUE 'archived';
  ```
  **Note**: Cannot be rolled back inside a transaction (committed immediately)

**DROP TYPE**:
- Fails if type is in use by tables
- Must drop dependent columns first

### Known Limitations

- `CREATE INDEX CONCURRENTLY` requires exclusive lock momentarily (may block on high-traffic tables)
- `ALTER TYPE ADD VALUE` cannot be rolled back
- Dropping columns does not reclaim disk space immediately (requires `VACUUM FULL`)
