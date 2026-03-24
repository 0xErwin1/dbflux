# dbflux_driver_postgres

## Features

- PostgreSQL relational driver with SQL query execution and schema discovery.
- Supports schemas, tables, views, indexes, foreign keys, check constraints, unique constraints, and custom types.
- Supports authentication, SSL, SSH tunneling, and URI/manual connection modes.
- Supports query cancellation through PostgreSQL cancel tokens.
- Includes PostgreSQL-specific SQL/code generation for CRUD, indexes, reindex, foreign keys, and type operations.

## Limitations

- SQL-only driver; it does not expose document or key-value APIs.
- Cancellation is best effort and depends on server/session state at cancellation time.
- Code generation targets supported PostgreSQL constructs only; unsupported generator IDs return `NotSupported`.

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
