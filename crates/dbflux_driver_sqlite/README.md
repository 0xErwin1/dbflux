# dbflux_driver_sqlite

## Features

- Embedded SQLite relational driver using file-based database paths.
- Supports SQL execution, schema discovery, views, indexes, foreign keys, check constraints, and unique constraints.
- Supports query cancellation via SQLite interrupt handles.
- Includes SQL/code generation for CRUD, indexes, reindex, create table, and drop table.

## Limitations

- Local file driver only; no network transport, SSH tunneling, or TLS/SSL mode.
- SQL-only driver; it does not expose document or key-value APIs.
- SQLite schema model has no server-side multi-schema namespace equivalent.

## DDL Capabilities

### Transactional DDL

SQLite supports **transactional DDL** — all DDL operations can be wrapped in transactions and rolled back:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone TEXT NULL;
-- Test the change
ROLLBACK;  -- Safe to rollback if something goes wrong
```

### ALTER TABLE Limitations

**CRITICAL**: SQLite has **very limited** `ALTER TABLE` support:

**Supported operations**:
- `ADD COLUMN` (at end of table only)
- `RENAME COLUMN` (SQLite 3.25.0+)
- `RENAME TABLE`

**NOT supported**:
- `DROP COLUMN` (requires table recreation)
- `ALTER COLUMN` (type change requires table recreation)
- `ADD COLUMN` in middle of table (requires table recreation)

### Table Recreation Pattern

For unsupported `ALTER TABLE` operations, use the table recreation pattern:

```sql
BEGIN;

-- 1. Create new table with desired schema
CREATE TABLE users_new (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL,
  name TEXT,
  -- phone column dropped, age column added
  age INTEGER
);

-- 2. Copy data from old table
INSERT INTO users_new (id, email, name, age)
  SELECT id, email, name, NULL FROM users;

-- 3. Drop old table
DROP TABLE users;

-- 4. Rename new table
ALTER TABLE users_new RENAME TO users;

COMMIT;
```

**IMPORTANT**: This pattern loses:
- Foreign key references from other tables
- Triggers on the original table
- Indexes on the original table (must recreate)

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

- No `DROP COLUMN` (requires table recreation)
- No `ALTER COLUMN` (requires table recreation)
- Cannot add constraints to existing tables
- No concurrent index creation (locks database)
- Dynamic typing (column types are advisory only)

### Best Practices

1. **Use transactions** — DDL is transactional, always wrap in `BEGIN`/`COMMIT`
2. **Plan schema ahead** — Difficult to modify later
3. **Use table recreation pattern** — For unsupported `ALTER TABLE` ops
4. **Recreate indexes and triggers** — After table recreation
5. **Test on copy first** — Especially for table recreation pattern
6. **Enable foreign keys** — `PRAGMA foreign_keys = ON` before altering schema
7. **Use VACUUM** — Reclaim disk space after `DROP TABLE` or table recreation
