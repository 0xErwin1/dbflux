# dbflux_driver_mssql

Microsoft SQL Server driver for DBFlux, built on the
[`tiberius`](https://crates.io/crates/tiberius) TDS client.

## Features

- SQL Server / Azure SQL relational driver with SQL query execution and schema
  discovery.
- Authentication via SQL Server logins (username + password); URI mode accepts
  ADO, JDBC, and `sqlserver://user:pass@host:port/db` connection strings.
- TLS encryption modes (`off`, `on`, `required`) via tiberius
  `EncryptionLevel`. The form exposes a single **SSL Mode** dropdown;
  the `TrustServerCertificate` flag is derived automatically:
  - `off` — no encryption (login packet still encrypted by TDS).
  - `on` — encrypted, accepts self-signed certs. Best for local/dev
    SQL Server with its auto-generated cert.
  - `required` — encrypted, validates the cert chain. Use against
    servers with a real CA-signed cert (Azure SQL, etc.).
  In URI mode, `?trust=true|false` explicitly overrides the derived
  value if you need an unusual combination (e.g. `?encrypt=required&trust=true`).
- Optional SQL Server named instances (`SQLEXPRESS`, `MSSQLSERVER2019`,
  etc.) resolved at connect time by querying SQL Browser on UDP 1434
  (enabled via tiberius's `sql-browser-tokio` feature). The form's
  Instance field, the SSMS-style `host\instance` form in URI mode, and
  the `?instance=` URI query parameter all set the same `instance_name`
  on the tiberius config.
- SSH tunnel support for connecting through bastion hosts (named instance
  lookup is not available through a TCP-only tunnel).
- Per-tab database switching via `USE [database]`; session state (SET options,
  temp tables, transactions) persists across `execute()` calls on the same
  connection.
- Multi-result-set batches: when a batch produces several result sets (e.g.
  `SELECT 1; SELECT 2;` or a stored procedure with multiple `SELECT`s), the
  driver returns the **last** non-empty set as the primary `QueryResult`
  (preserving the historical "last statement wins" UX) and attaches every
  earlier non-empty set to `QueryResult.additional_results` in batch order.
  Pure preparation batches (`SET LOCK_TIMEOUT 5000`) still surface as a
  single empty primary. Callers that want to walk every set use
  `QueryResult::iter_result_sets()`.

### Instance Metrics

Exposes a curated set of live server metrics sourced from `sys.dm_os_performance_counters`:

- `mssql.batch_requests_per_sec` — T-SQL batch requests per second
- `mssql.compilations_per_sec` — SQL compilations per second
- `mssql.recompilations_per_sec` — SQL re-compilations per second
- `mssql.user_connections` — current open user connections
- `mssql.lock_waits_per_sec` — lock waits per second (`_Total` instance)
- `mssql.page_reads_per_sec` — buffer pool page reads per second
- `mssql.page_writes_per_sec` — buffer pool page writes per second
- `mssql.buffer_cache_hit_ratio` — buffer cache hit ratio (percent)
- `mssql.server_memory_kb` — total server memory in KB

Each metric is returned as a single `(timestamp_ms, value)` row for live charting.

Requires the `VIEW SERVER STATE` server permission. Without it, `list_metrics()` returns an empty list and a warning is logged. The driver probes this permission once at catalog construction time.

### Instance Inspector

Exposes tabular snapshots of running server state:

- `mssql.active_sessions` — user sessions from `sys.dm_exec_sessions` joined with `sys.dm_exec_requests` (session id, login name, host name, program name, status, CPU time, memory usage, command, request status, wait type, wait time, blocking session id)

Requires the `VIEW SERVER STATE` permission.

### Query cancellation

- Cancellation is implemented as `KILL <spid>` issued from a fresh
  side-channel connection. tiberius does not currently expose the TDS
  Attention primitive that SSMS uses, so the next best option is to ask
  the server to terminate the session running the query.
- At connect time the driver captures `@@SPID` and caches a clone of the
  tiberius `Config` (with the login already baked in). The cancel handle
  opens a second connection on demand, runs `KILL <spid>`, and marks the
  primary connection as poisoned.
- After cancellation, `cleanup_after_cancel()` rebuilds the primary tiberius
  client, captures the new SPID, and re-issues the previous `USE [db]` so
  the next query runs in the same database. From the UI's perspective the
  connection stays connected; only the underlying session id changes.
- Errors raised on the killed session (codes 596 / 233 / 6005) are
  translated to `DbError::Cancelled` so the UI shows "query cancelled"
  rather than a transport-level failure.
- The session owner can `KILL` its own SPID on modern SQL Server without
  the `ALTER ANY CONNECTION` permission. On older or restricted logins,
  the KILL itself may fail with a permission error; the driver surfaces
  that to the user.

### Schema discovery

- Databases (`sys.databases`, hides system DBs).
- Tables and views per database (`sys.tables`, `sys.views`).
- Per-table columns + primary key flag, indexes, foreign keys.
- Per-table constraints: CHECK constraints (with their definition) and
  UNIQUE constraints (via `sys.indexes.is_unique_constraint`).
- Whole-schema indexes and foreign keys for the schema browser sidebar.
- User-defined types (`sys.types where is_user_defined = 1`) classified as
  `Domain` (alias types) or `Composite` (table types).
- `view_details()` verifies the view exists in the requested database.
- **Routines:** stored procedures (`P`), scalar functions (`FN`), inline
  table-valued functions (`IF`), multi-statement table-valued functions (`TF`),
  and CLR aggregates (`AF`) are listed per schema via `sys.objects`. Source
  definitions are fetched with `OBJECT_DEFINITION(object_id)`.

### CRUD with OUTPUT

- INSERT/UPDATE/DELETE on a row use SQL Server's `OUTPUT INSERTED.*` /
  `OUTPUT DELETED.*` clause so the post-mutation row data is returned to the
  caller (`CrudResult::success(row)`), the same way the Postgres driver uses
  `RETURNING *`.
- `MutationCapabilities::supports_returning` is `true`.
- Row identity must be a composite primary key (the only `RecordIdentity`
  variant that makes sense for a relational driver).

### Query planning

- `explain()` runs the query under `SET SHOWPLAN_XML ON` and returns the
  query plan as XML. The driver always runs `SET SHOWPLAN_XML OFF` afterward
  so session state does not leak.
- `version_query()` returns `SELECT @@VERSION`.

### Dialect

- `[bracket]` identifier quoting with `]` escaping.
- `N'…'` Unicode string literals; `0x…` (uppercase) binary literals;
  `1`/`0` for boolean (`BIT`) values.
- `OFFSET … ROWS FETCH NEXT … ROWS ONLY` pagination (with a fallback
  `ORDER BY 1` so OFFSET-without-ORDER-BY queries do not error out).
- `SELECT TOP N` is not used; OFFSET/FETCH is the canonical pagination form.
- `UPSERT` is intentionally not generated; SQL Server's `MERGE` has known
  bugs and should be written by hand.

### Error reporting

- Tiberius `Server` token errors surface their numeric code, severity state,
  and source line through `FormattedError`.
- Common MSSQL error numbers are mapped to semantic `DbError` variants
  rather than the generic `QueryFailed`:

  | Code(s)                                       | DbError variant         |
  | --------------------------------------------- | ----------------------- |
  | 4060, 18450, 18452, 18456, 18486, 18487, 18488 | `AuthFailed`            |
  | 229, 230, 262, 297, 916                       | `PermissionDenied`      |
  | 207, 208, 2812, 4902                          | `ObjectNotFound`        |
  | 245, 334, 515, 547, 2601, 2627, 8152          | `ConstraintViolation`   |
  | 102, 156, 8180                                | `SyntaxError`           |

- Constraint-violation messages are parsed to populate `ErrorLocation`
  (schema, table, column, constraint name) so the UI can highlight the
  offending object.

### Operations and limits

- All operations declare `transactional_ddl: true` and `supports_savepoints:
  true`.
- Supported isolation levels: ReadUncommitted, ReadCommitted, RepeatableRead,
  Serializable, Snapshot. Default is ReadCommitted.

## DDL behavior

- **Transactional DDL.** Most DDL in SQL Server is transactional. Wrapping
  `CREATE`, `ALTER`, or `DROP TABLE` inside `BEGIN TRAN … COMMIT` /
  `ROLLBACK` works. Exceptions: `CREATE DATABASE`, `DROP DATABASE`, `ALTER
  DATABASE`, `BACKUP`/`RESTORE`, and `CREATE FULLTEXT INDEX` cannot run
  inside an explicit transaction.
- **ALTER TABLE locking.** `ALTER TABLE … ADD COLUMN <nullable>` is fast
  (metadata-only). Adding a NOT NULL column with a default writes to every
  page and takes a Sch-M lock. `ALTER TABLE … ALTER COLUMN` may rewrite the
  table and blocks reads and writes until it finishes.
- **Online index operations** (Enterprise / Azure SQL): `CREATE INDEX … WITH
  (ONLINE = ON)` and `ALTER INDEX … REBUILD WITH (ONLINE = ON)` allow
  concurrent DML. Without `ONLINE = ON`, index builds take Sch-M and block
  writes (Standard/Express editions only support offline).
- **TRUNCATE TABLE.** Metadata-only, fast, transactional, requires
  `ALTER` permission on the table. Cannot be used on tables referenced by a
  foreign key (use `DELETE` or drop the FK first).
- **DROP TABLE / DROP VIEW.** Transactional. `IF EXISTS` is supported on
  2016+.
- **Constraints.** Adding `CHECK` / `UNIQUE` / `FOREIGN KEY` constraints
  validates all existing rows by default (takes Sch-M briefly). Use `WITH
  NOCHECK` to add the constraint without scanning, then `WITH CHECK CHECK
  CONSTRAINT` later to validate at your leisure — same pattern as Postgres's
  `NOT VALID` + `VALIDATE CONSTRAINT`.

## Limitations

- Instance metrics and inspector features require the `VIEW SERVER STATE` server permission. Without it, both `list_metrics()` and `list_inspectors()` return empty lists rather than an error.

- Instance metrics return a single data point per call (current value from `sys.dm_os_performance_counters`), not a historical time series. Rate counters (e.g. `mssql.batch_requests_per_sec`) represent the server-side running average as reported by the DMV, not a delta computed by the driver.

- Minimum supported SQL Server: 2016 (13.0). The driver uses
  `DROP INDEX IF EXISTS … ON …` syntax that older servers reject with a
  syntax error (102). Azure SQL Database and Managed Instance are fine.
- CRUD on tables (or updateable views) with `INSTEAD OF` triggers is not
  supported. The driver returns the post-mutation row via
  `OUTPUT INSERTED.*` / `OUTPUT DELETED.*` without an `INTO` clause, which
  SQL Server rejects with error 334 ("the target table cannot have any
  enabled triggers if the statement contains an OUTPUT clause without
  INTO"). The error is surfaced as `ConstraintViolation`.
- SQL-only driver; it does not expose document or key-value APIs.
- Cancellation kills the underlying session and transparently reconnects;
  it is not the surgical TDS-Attention cancel that SSMS uses (tiberius does
  not currently expose that primitive). In practice the only user-visible
  difference is that any session-local state (`SET` options, temp tables,
  open transactions) is reset by the cancel. The active database is
  restored automatically.
- Cancellation latency depends on the SQL Server scheduler: typically a few
  milliseconds for CPU-bound queries, immediate for lock-waiters. Long
  rollbacks (e.g. cancelling a large `DELETE` mid-transaction) can keep the
  *server-side* SPID in the KILLED/ROLLBACK state for a while after the
  driver has already moved on to a fresh session.
- Parameter binding is not used — statements are dispatched via
  `simple_query`. CRUD helpers compose values into the SQL text through the
  shared `SqlQueryBuilder` and dialect literal formatters. Large binary or
  Unicode payloads are inlined as `0x…` or `N'…'` literals.
- Streaming: result sets are materialized into `Vec<Row>`. The
  `Connection::execute` trait returns a fully-resolved `QueryResult`, so
  cursor-style streaming would require a workspace-level API change rather
  than a driver-only fix.
- Multi-statement batches surface every non-empty result set via
  `QueryResult.additional_results`, but the UI currently renders only the
  primary (last) set. Until the result-tab system reads
  `additional_results`, the earlier sets are captured by the driver but
  invisible in the editor.
- `PRINT` and informational messages emitted during a batch are dropped.
  Surfacing them would require driving tiberius's lower-level
  `TokenStream` instead of `QueryStream::into_results()`.
- `UPSERT` is intentionally not generated. Use `MERGE` manually when needed.
- Named instances are honored when connecting directly (tiberius queries the
  SQL Browser service on UDP 1434) but not when going through an SSH tunnel,
  since libssh2 only forwards TCP. The standard workaround is to assign a
  static TCP port to the instance and connect to that port directly.
- Schema introspection uses the `sys.*` catalog views; users without the
  default `VIEW DEFINITION` permission will see partial metadata. SQL
  Server's metadata visibility rules apply.
- CLR routines (CLR scalar functions, CLR table-valued functions, CLR stored
  procedures) and any routine created with `ENCRYPTION` return `NULL` from
  `OBJECT_DEFINITION`, in which case the driver surfaces a short fallback
  message rather than an error.
- `parameter_types` is not populated for routines; `sys.parameters` is not
  queried in this implementation.
- SQL Server has no `Window` function kind in the `sys.objects.type` taxonomy;
  `RoutineKind::Window` is never emitted by this driver.
