# SQLite

Embedded file-based database.

## At a glance

- **Category** — Relational
- **Query language** — SQL
- **URI scheme** — `sqlite`

## Features

- Embedded SQLite relational driver using file-based database paths.
- Supports SQL execution, schema discovery, views, indexes, foreign keys, check constraints, and unique constraints.
- Supports query cancellation via SQLite interrupt handles.
- Includes SQL/code generation for CRUD, indexes, reindex, create table, and drop table.
- Multi-statement scripts (several `;`-separated statements) are split and executed statement by statement, each through the typed prepared path, returning one result set per statement. (`rusqlite::prepare` only parses the first statement of a string, so a script must be split.)
- Enforces a requested row limit on every single statement that produces rows (`SELECT`, `PRAGMA`, `EXPLAIN`, `WITH ... SELECT`, `VALUES`, and DML with `RETURNING`) by retaining only the requested rows during iteration: iteration drains to completion, so a mutation's effects always finish fully, a late per-row error still surfaces, and the result reports when rows were omitted.
- Runs a row-limited multi-statement batch statement by statement under one row budget shared by the whole request. The batch is split with SQLite's own lexer, so a backslash is not an escape and semicolons inside literals, comments, and trigger bodies do not split a statement; trailing comments or semicolons do not create an extra statement. Each row-producing statement retains rows only while the budget lasts, statements after the budget is exhausted still run, and the batch stops at the first failure exactly as an unbounded batch does.
- Rejects, before any preparation or execution, requests it cannot bound safely: a row-limited request aimed at instance metrics or inspectors, and a requested statement timeout.
- Data-transfer engine: native multi-row `INSERT` bulk-load (`BULK_INSERT`), driver-native `CREATE TABLE` DDL from a source table's columns, and a per-connection referential-integrity toggle (`PRAGMA foreign_keys`) for FK-safe migrations.

## Limitations

- Local file driver only; no network transport, SSH tunneling, or TLS/SSL mode.
- A requested row limit is a retention cap, not an engine bound: the statement still runs to completion inside the embedded engine, all rows past the cap are observed and discarded, and no byte, memory, or time budget is enforced — sorter and `RETURNING` allocations are not bounded by the cap. A mutation under a row limit completes all of its effects.
- Splitting a row-limited batch prepares nothing, but probes the statement text with SQLite's lexer at every `;`, which is quadratic in the length of a statement that holds many semicolons inside literals, comments, or trigger bodies. Unbounded batches keep the legacy split-and-run behavior.
- Without a row limit, `WITH ... SELECT`, `VALUES`, and DML with `RETURNING` keep the legacy behavior: they report an unsupported-execution error after the statement has already run.
- A row-limited request aimed at the driver's instance metrics or inspectors is refused before the connection lock or any dispatch — the public request type can carry those contexts although the driver advertises no instance catalog — while uncapped requests keep the existing behavior.
- A requested statement timeout (`QueryRequest::statement_timeout`) is unsupported and rejected before execution. Ordinary uncapped queries remain cancellable through the existing interrupt path.
- SQL-only driver; it does not expose document or key-value APIs.
- SQLite schema model has no server-side multi-schema namespace equivalent.
- No `TRUNCATE TABLE` statement; the data-transfer engine's Truncate load option is unavailable for SQLite targets (`DriverCapabilities::TRUNCATE_TABLE` is not set).

## DDL Capabilities

### Transactional DDL

SQLite supports **transactional DDL** — manual DDL operations can be wrapped in transactions and rolled back:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone TEXT NULL;
-- Test the change
ROLLBACK;  -- Safe to rollback if something goes wrong
```

Managed table alterations are different: their planner requires an autocommit
connection because the driver owns the per-table transaction.

### Managed ALTER TABLE

For a pure `DROP COLUMN` request, DBFlux uses SQLite's native `DROP COLUMN`
path first when the linked SQLite version supports it. Native drops keep SQLite's
own acceptance rules; they are not limited by the rebuild grammar. DBFlux still
preflights known dependencies and SQLite validates the request again at execution.

For selected type, nullability, or default changes, and for a pure drop when the
linked SQLite version has no native `DROP COLUMN`, DBFlux uses a conservative
rebuild of one `main`-schema table. A rebuild request may also combine those
column changes with drops.

The rebuild copies retained rows without casts, conversion, or backfill. It
preserves the proven row identity and stored values exactly; a changed default
applies only to future inserts. Making a column required can therefore fail when
existing rows are incompatible.

Preparation is read-only. The preview exposed through the generic UI and MCP is
an immutable, illustrative lifecycle description, not executable apply SQL. The
driver manages the private replacement table, copy and exact comparison, source
replacement, proven explicit-index restoration, and final validation.

Each rebuild is atomic for its one table. On failure, the driver verifies rollback
and restoration of connection settings. Cleanup failures remain visible; uncertain
transaction state or failed connection-setting restoration quarantines the
connection until reconnection. There is no cross-table atomicity.

#### Rebuild scope

The rebuild route accepts only table shapes it can prove safe in the `main`
schema. It rejects, for example, CHECK constraints, generated columns,
`AUTOINCREMENT`, `STRICT`, `WITHOUT ROWID`, views or triggers, expression or
partial indexes, attached schemas, and active caller-owned transactions. These
limits apply to rebuilds only: an otherwise applicable native pure drop is not
rejected merely because it is outside the rebuild grammar.

### Index Operations

**CREATE INDEX**:
- Locks database for duration (blocks writes)
- No concurrent option (unlike PostgreSQL)

**DROP INDEX**:
- Fast (metadata-only)

**REINDEX**:
- Rebuilds index (locks database)

### Constraints

**Adding constraints**:
- SQLite validates constraints at `INSERT`/`UPDATE` time
- Cannot add constraints to existing tables (requires table recreation)

**Foreign keys**:
- Disabled by default (must enable with `PRAGMA foreign_keys = ON`)
- Cannot be added to existing tables (requires table recreation)

### Known Limitations

- The managed table-alter planner changes selected type, nullability, defaults, and drops; it does not add constraints to an existing table.
- Rebuilds are deliberately limited to the proven table shapes described above.
- No concurrent index creation (locks database)
- Dynamic typing (column types are advisory only)

### Best Practices

1. **Use transactions for manual DDL** — managed table alterations require autocommit.
2. **Plan schema ahead** — Difficult to modify later.
3. **Review the managed ALTER preview** — it is descriptive; apply through DBFlux rather than running its statements.
4. **Plan unsupported shapes separately** — the rebuild route rejects views, triggers, and other unproven shapes.
5. **Test on a copy first** — especially before a managed rebuild.
6. **Enable foreign keys** — `PRAGMA foreign_keys = ON` before altering schema.
7. **Use VACUUM** — Reclaim disk space after `DROP TABLE` or table recreation.
