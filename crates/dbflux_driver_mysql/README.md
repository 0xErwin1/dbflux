# dbflux_driver_mysql

## Features

- MySQL and MariaDB relational driver implementations in one crate.
- Supports SQL execution, schema discovery, indexes, foreign keys, check constraints, and unique constraints.
- Supports authentication, SSH tunneling, and URI/manual connection modes.
- TLS with the five native SSL modes (`DISABLED`, `PREFERRED`, `REQUIRED`, `VERIFY_CA`, `VERIFY_IDENTITY`): `VERIFY_CA` verifies the server chain while skipping hostname validation and `VERIFY_IDENTITY` verifies both. A custom root CA replaces the system trust store for the verifying modes, and a client certificate + key enables mutual TLS. Uses the `rustls`/`aws-lc-rs` backend.
- Supports query cancellation through a dedicated cancel path (`KILL QUERY` flow).
- Includes SQL/code generation for CRUD, indexes, foreign keys, and table DDL operations.
- Routine discovery: lists stored procedures and user-defined functions from `information_schema.ROUTINES` including parameter types and return type hints (Functions only).
- Routine definition: retrieves the full `CREATE FUNCTION` or `CREATE PROCEDURE` body via `SHOW CREATE FUNCTION`/`SHOW CREATE PROCEDURE` (read-only; definition is not editable or executable in the viewer).
- Multi-statement scripts (several `;`-separated statements) are split and executed statement by statement, each through the typed prepared path, returning one result set per statement.
- Data-transfer engine: native multi-row `INSERT` bulk-load (`BULK_INSERT`), driver-native `CREATE TABLE` DDL from a source table's columns, `TRUNCATE TABLE` support, and a referential-integrity toggle (`SET FOREIGN_KEY_CHECKS`) for FK-safe migrations. Both MySQL and MariaDB share this support.

### Instance Metrics

Exposes a curated set of live server metrics sourced from `SHOW GLOBAL STATUS`:

- `mysql.threads_connected` — current open connections
- `mysql.threads_running` — currently executing queries
- `mysql.queries_per_sec` — queries per second (cumulative counter)
- `mysql.innodb_buffer_pool_hit_ratio` — InnoDB buffer pool read efficiency
- `mysql.innodb_rows_read` — rows read from InnoDB storage engine
- `mysql.innodb_rows_inserted` — rows inserted into InnoDB
- `mysql.innodb_rows_updated` — rows updated in InnoDB
- `mysql.innodb_rows_deleted` — rows deleted from InnoDB
- `mysql.slow_queries` — cumulative slow query count
- `mysql.table_locks_waited` — table-level lock contention counter
- `mysql.bytes_sent` — network bytes sent

Each metric is returned as a single `(timestamp_ms, value)` row for live charting.

### Instance Inspector

Exposes tabular snapshots of running server state:

- `mysql.processlist` — active sessions from `information_schema.PROCESSLIST` (user, host, db, command, time, state, info)

## Limitations

- SQL-only driver; it does not expose document or key-value APIs.

- Instance metrics return a single data point per call (current snapshot from `SHOW GLOBAL STATUS`), not a historical time series. Cumulative counters (e.g. `mysql.bytes_sent`) grow monotonically — interpret them as deltas between samples rather than absolute rates.

- The `performance_schema` availability probe runs once at catalog construction time. When `performance_schema` is absent, performance-schema-specific metrics are omitted from `list_metrics()`. The static metric set (`SHOW GLOBAL STATUS` based) is always available.

- A multi-statement script runs each statement sequentially rather than as one atomic server-side batch; statement splitting is text-based and may missplit stored-program bodies that embed `;` (e.g. `CREATE PROCEDURE ... BEGIN ... END`).
- Cancellation depends on server permissions and connection state when `KILL QUERY` is issued.
- Code generation is scoped to supported MySQL/MariaDB constructs; unsupported generator IDs return `NotSupported`.
- Routine listing covers only FUNCTION and PROCEDURE types. MySQL aggregate functions (registered via `CREATE AGGREGATE FUNCTION` UDF plugin) and window functions are not surfaced in `information_schema.ROUTINES` and are therefore not listed.
- `SHOW CREATE FUNCTION`/`SHOW CREATE PROCEDURE` requires the `SHOW_ROUTINE` privilege (MySQL 8.0+) or ownership of the routine; without sufficient privileges the definition column returns `NULL` and the viewer displays a notice instead of the source.

## DDL Capabilities

### Non-Transactional DDL

**CRITICAL**: MySQL DDL operations are **NOT transactional** — they cannot be rolled back:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;
-- DDL is committed immediately, ROLLBACK has no effect!
ROLLBACK;  -- Too late, column already added
```

**Exception**: `RENAME TABLE` is atomic (safe to use in transactions).

### ALTER TABLE Behavior

**Table rewrites**:
- Most `ALTER TABLE` operations rewrite the entire table (locks table for duration)
- Use `ALGORITHM=INPLACE` and `LOCK=NONE` for online DDL (MySQL 5.6+):
  ```sql
  ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL, ALGORITHM=INPLACE, LOCK=NONE;
  ```

**Adding columns**:
- Adding column at **end of table**: Fast (metadata-only)
- Adding column in **middle of table**: Table rewrite (locks table)
- Use `AFTER column_name` to control position

**Adding columns with defaults**:
- Table rewrite (locks table)
- Default value is written to all existing rows

**Changing column types**:
- Always requires table rewrite (locks table)
- Data conversion happens during rewrite

**Dropping columns**:
- Table rewrite (locks table)
- Data is immediately deleted

**Renaming columns**:
- Table rewrite (locks table)
- May break views, triggers, and application code

### Index Operations

**CREATE INDEX**:
- Locks table for writes (reads allowed)
- Use `ALGORITHM=INPLACE, LOCK=NONE` for online index creation:
  ```sql
  CREATE INDEX idx_users_email ON users(email) ALGORITHM=INPLACE, LOCK=NONE;
  ```

**DROP INDEX**:
- Locks table for writes (reads allowed)
- Use `ALGORITHM=INPLACE, LOCK=NONE` for online index removal

### Constraints

**Foreign keys**:
- Adding foreign keys scans both tables (locks both)
- Use `ALGORITHM=INPLACE, LOCK=NONE` when possible

**UNIQUE constraints**:
- Requires index creation (locks table)

**CHECK constraints** (MySQL 8.0.16+):
- Metadata-only (fast)
- Validated on INSERT/UPDATE only

### Online DDL (MySQL 5.6+)

**ALGORITHM options**:
- `INPLACE` — Modify table in place (no copy)
- `COPY` — Create new table and copy rows (default for old MySQL versions)
- `INSTANT` — Metadata-only (MySQL 8.0+, limited operations)

**LOCK options**:
- `NONE` — Allow concurrent reads and writes
- `SHARED` — Allow reads, block writes
- `EXCLUSIVE` — Block reads and writes

**Example**:
```sql
ALTER TABLE users 
  ADD COLUMN phone VARCHAR(20) NULL,
  ALGORITHM=INPLACE,
  LOCK=NONE;
```

### Known Limitations

- DDL not transactional (cannot rollback)
- Most `ALTER TABLE` ops rewrite entire table (locks table)
- Adding column in middle of table requires rewrite
- Online DDL support varies by MySQL version
- Use `pt-online-schema-change` (Percona Toolkit) for zero-downtime DDL on large tables

### Best Practices

1. **Test on a copy first** — DDL cannot be rolled back
2. **Use online DDL** — Add `ALGORITHM=INPLACE, LOCK=NONE` when supported
3. **Schedule maintenance windows** — Run DDL during low-traffic periods
4. **Monitor table size** — Large tables take longer to rewrite
5. **Use pt-online-schema-change** — For zero-downtime DDL on production tables
