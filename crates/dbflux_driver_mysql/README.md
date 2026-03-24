# dbflux_driver_mysql

## Features

- MySQL and MariaDB relational driver implementations in one crate.
- Supports SQL execution, schema discovery, indexes, foreign keys, check constraints, and unique constraints.
- Supports authentication, SSL, SSH tunneling, and URI/manual connection modes.
- Supports query cancellation through a dedicated cancel path (`KILL QUERY` flow).
- Includes SQL/code generation for CRUD, indexes, foreign keys, and table DDL operations.

## Limitations

- SQL-only driver; it does not expose document or key-value APIs.
- Cancellation depends on server permissions and connection state when `KILL QUERY` is issued.
- Code generation is scoped to supported MySQL/MariaDB constructs; unsupported generator IDs return `NotSupported`.

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
