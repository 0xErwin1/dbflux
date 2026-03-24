# DDL Safety Guide

Complete reference for DDL (Data Definition Language) operations in DBFlux MCP.

## Table of Contents

- [Overview](#overview)
- [Safety Classification System](#safety-classification-system)
- [DDL Preview System](#ddl-preview-system)
- [Safety Matrix](#safety-matrix)
- [Governance Integration](#governance-integration)
- [ALTER TABLE Safety](#alter-table-safety)
- [CREATE/DROP Operations](#createdrop-operations)
- [Index Operations](#index-operations)
- [Testing DDL Changes](#testing-ddl-changes)
- [Driver-Specific Behavior](#driver-specific-behavior)
- [Best Practices](#best-practices)
- [Error Recovery](#error-recovery)

## Overview

DDL operations modify database schema and structure, making them inherently higher risk than data queries. DBFlux implements a multi-layered safety system:

1. **Classification** — Every DDL operation is classified by risk level
2. **Preview** — DDL can be previewed before execution (dry-run)
3. **Governance** — Policy engine controls who can execute DDL
4. **Approval** — Destructive DDL can require human approval
5. **Audit** — All DDL operations are logged

This guide explains how each layer works and how AI agents should use DDL tools safely.

## Safety Classification System

DBFlux uses `ExecutionClassification` to categorize operations by risk:

### Classification Levels

| Level | Risk | Examples | Requires Approval? |
|-------|------|----------|-------------------|
| `Metadata` | None | `list_tables`, `describe_object` | No |
| `Read` | None | `select_data`, `count_records` | No |
| `Write` | Low | `insert_record`, `update_records` | Policy-dependent |
| `Destructive` | High | `delete_records`, `truncate_table` | Policy-dependent |
| `AdminSafe` | Medium | `create_table`, `create_index`, `alter_table` (add column) | Policy-dependent |
| `Admin` | High | `alter_table` (rename/drop column), `drop_index` | Yes (default) |
| `AdminDestructive` | Very High | `drop_table`, `drop_database` | Yes (always) |

### Classification Rules

**AdminSafe** — Schema changes that are safe and reversible:
- `CREATE TABLE` (empty table)
- `CREATE INDEX`
- `ALTER TABLE ADD COLUMN` (with default)
- `ALTER TABLE ADD CONSTRAINT` (validation-only)

**Admin** — Schema changes that may affect existing data or are hard to reverse:
- `ALTER TABLE DROP COLUMN`
- `ALTER TABLE RENAME COLUMN`
- `ALTER TABLE ALTER COLUMN` (type change)
- `ALTER TABLE DROP CONSTRAINT`
- `DROP INDEX`

**AdminDestructive** — Schema changes that destroy data irreversibly:
- `DROP TABLE`
- `DROP DATABASE`
- `TRUNCATE TABLE`

### Classification Algorithm

`classify_alter_table_operation()` in `dbflux_core/src/query/classify.rs`:

```rust
pub fn classify_alter_table_operation(operation: &AlterOperation) -> ExecutionClassification {
    use AlterOperationAction::*;
    match operation.action {
        AddColumn => {
            // Adding a column is safe if it has a default or is nullable
            if operation.definition.as_ref().map(|d| d.default.is_some() || d.nullable == Some(true)).unwrap_or(false) {
                ExecutionClassification::AdminSafe
            } else {
                // Adding a non-nullable column without default requires backfill
                ExecutionClassification::Admin
            }
        }
        DropColumn | RenameColumn => ExecutionClassification::Admin,
        AlterColumn => {
            // Type changes are Admin (may require data migration)
            ExecutionClassification::Admin
        }
        AddConstraint => {
            // Validation constraints are AdminSafe
            // Foreign keys that cascade delete are Admin
            if operation.definition.as_ref().map(|d| d.cascade).unwrap_or(false) {
                ExecutionClassification::Admin
            } else {
                ExecutionClassification::AdminSafe
            }
        }
        DropConstraint => ExecutionClassification::Admin,
    }
}
```

## DDL Preview System

The `preview_mutation` tool allows AI agents to see what SQL/query will be executed without running it.

### Preview Workflow

1. **Agent calls `preview_mutation`** with operation parameters
2. **DBFlux generates SQL** using `QueryGenerator` trait
3. **Preview returned** with SQL, affected objects, and classification
4. **Agent reviews** and decides whether to proceed
5. **Agent calls actual tool** (`alter_table`, `create_table`, etc.) if safe

### Preview Example

**Request:**

```json
{
  "tool": "alter_table",
  "connection_id": "prod-db",
  "database": "public",
  "table": "users",
  "operations": [
    {
      "action": "add_column",
      "column": "phone",
      "definition": {
        "type": "varchar(20)",
        "nullable": true
      }
    }
  ]
}
```

**Preview Response:**

```json
{
  "classification": "admin_safe",
  "sql": "ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;",
  "affected_objects": ["users"],
  "estimated_impact": "low",
  "reversible": true,
  "warnings": []
}
```

**Request (Destructive):**

```json
{
  "tool": "alter_table",
  "connection_id": "prod-db",
  "database": "public",
  "table": "users",
  "operations": [
    {
      "action": "drop_column",
      "column": "legacy_id"
    }
  ]
}
```

**Preview Response:**

```json
{
  "classification": "admin",
  "sql": "ALTER TABLE users DROP COLUMN legacy_id;",
  "affected_objects": ["users", "users.legacy_id"],
  "estimated_impact": "high",
  "reversible": false,
  "warnings": [
    "Dropping a column is irreversible",
    "Data in 'legacy_id' will be permanently deleted",
    "Foreign keys referencing 'legacy_id' may fail"
  ]
}
```

### Preview Fields

| Field | Type | Description |
|-------|------|-------------|
| `classification` | String | Risk level (`metadata`, `read`, `write`, `destructive`, `admin_safe`, `admin`, `admin_destructive`) |
| `sql` | String | Generated SQL/query (driver-dependent) |
| `affected_objects` | Array | Tables, columns, indexes, constraints affected |
| `estimated_impact` | String | `low`, `medium`, `high` (based on table size, indexes, constraints) |
| `reversible` | Boolean | Whether operation can be undone easily |
| `warnings` | Array | Risk warnings and recommendations |

## Safety Matrix

### ALTER TABLE Operations

| Operation | Classification | Reversible | Risks | Recommendations |
|-----------|---------------|------------|-------|-----------------|
| `ADD COLUMN` (nullable) | `AdminSafe` | Yes (drop column) | None | Safe for production |
| `ADD COLUMN` (non-null, no default) | `Admin` | Yes | Requires backfill | Add default or make nullable |
| `ADD COLUMN` (non-null, with default) | `AdminSafe` | Yes | May lock table | Test on staging first |
| `DROP COLUMN` | `Admin` | No | Data loss | Backup before dropping |
| `RENAME COLUMN` | `Admin` | Yes | Application breakage | Update application code first |
| `ALTER COLUMN` (type change) | `Admin` | Depends | Data loss if incompatible | Test type conversion on copy |
| `ADD CONSTRAINT` (CHECK) | `AdminSafe` | Yes | May fail if data invalid | Validate data first |
| `ADD CONSTRAINT` (FK) | `AdminSafe` | Yes | May fail if orphans exist | Clean orphaned records first |
| `ADD CONSTRAINT` (FK CASCADE DELETE) | `Admin` | Yes | Cascading deletes | Review cascade behavior |
| `DROP CONSTRAINT` | `Admin` | Yes (re-add) | Data integrity loss | Document constraint logic |

### CREATE/DROP Operations

| Operation | Classification | Reversible | Risks | Recommendations |
|-----------|---------------|------------|-------|-----------------|
| `CREATE TABLE` | `AdminSafe` | Yes (drop table) | None | Safe for production |
| `CREATE TABLE IF NOT EXISTS` | `AdminSafe` | Yes | None | Idempotent, safe to retry |
| `DROP TABLE` | `AdminDestructive` | No | Data loss | Backup before dropping |
| `DROP TABLE IF EXISTS` | `AdminDestructive` | No | Data loss | Idempotent, still destructive |
| `TRUNCATE TABLE` | `AdminDestructive` | No | Data loss | Use `DELETE` if recovery needed |
| `CREATE INDEX` | `AdminSafe` | Yes (drop index) | Table lock (PostgreSQL < 11) | Use `CONCURRENTLY` if available |
| `DROP INDEX` | `Admin` | Yes (re-create) | Query performance loss | Verify index not in use |

### PostgreSQL-Specific

| Operation | Classification | Notes |
|-----------|---------------|-------|
| `CREATE INDEX CONCURRENTLY` | `AdminSafe` | Does not lock table; safe for production |
| `DROP INDEX CONCURRENTLY` | `Admin` | Does not lock table; safe for production |
| `ALTER TABLE ADD COLUMN` (with default) | `Admin` | PostgreSQL 11+ rewrites table; may lock for minutes |
| `CREATE TYPE` | `AdminSafe` | Safe, but `DROP TYPE` is `Admin` |
| `ALTER TYPE ADD VALUE` | `AdminSafe` | Safe, but cannot be rolled back in transaction |

### MySQL-Specific

| Operation | Classification | Notes |
|-----------|---------------|-------|
| `ALTER TABLE` (any) | `Admin` | MySQL rewrites entire table; locks for duration |
| `ALTER TABLE ... ALGORITHM=INPLACE` | `AdminSafe` | MySQL 5.6+ avoids table rewrite |
| `ALTER TABLE ... LOCK=NONE` | `AdminSafe` | Allows concurrent reads/writes |
| `RENAME TABLE` | `AdminSafe` | Atomic, no data copy |

### SQLite-Specific

| Operation | Classification | Notes |
|-----------|---------------|-------|
| `ALTER TABLE` (limited) | `Admin` | SQLite only supports `ADD COLUMN` and `RENAME` |
| `DROP COLUMN` | Not supported | Must recreate table manually |
| `ALTER COLUMN` | Not supported | Must recreate table manually |
| `CREATE INDEX` | `AdminSafe` | No concurrent option; locks database |

## Governance Integration

### Policy Evaluation

When an AI agent calls a DDL tool, the policy engine evaluates:

1. **Actor ID** — Who is requesting the operation (AI client ID)
2. **Connection ID** — Which database connection
3. **Tool ID** — Which MCP tool (`alter_table`, `drop_table`, etc.)
4. **Classification** — Risk level (`AdminSafe`, `Admin`, `AdminDestructive`)

**Policy decision:**
- `Allow` — Execute immediately
- `Deny` — Reject with reason
- `RequireApproval` — Queue for human approval

### Default Policy

DBFlux ships with a default policy:

```yaml
roles:
  - name: ai_agent_default
    policies:
      - tool: "*"
        classification: [metadata, read]
        decision: allow
      - tool: "*"
        classification: [write]
        decision: allow
        max_rows: 1000
      - tool: "*"
        classification: [destructive]
        decision: require_approval
      - tool: "*"
        classification: [admin_safe]
        decision: allow
      - tool: "*"
        classification: [admin, admin_destructive]
        decision: require_approval
```

**Interpretation:**
- Metadata and read operations: Always allowed
- Write operations: Allowed up to 1000 rows
- Destructive data operations: Require approval
- Safe DDL (AdminSafe): Allowed
- Risky DDL (Admin, AdminDestructive): Require approval

### Custom Policies

Users can define custom policies in the MCP settings:

```json
{
  "trusted_clients": [
    {
      "client_id": "trusted-ai-agent",
      "role": "trusted_developer"
    }
  ],
  "roles": [
    {
      "name": "trusted_developer",
      "policies": [
        {
          "tool": "alter_table",
          "classification": ["admin_safe", "admin"],
          "decision": "allow"
        },
        {
          "tool": "drop_table",
          "classification": ["admin_destructive"],
          "decision": "deny",
          "reason": "Use staging environment for DROP TABLE"
        }
      ]
    }
  ]
}
```

### Connection-Scoped Policies

Policies can be scoped to specific connections:

```json
{
  "roles": [
    {
      "name": "prod_restricted",
      "policies": [
        {
          "connection_id": "prod-db",
          "classification": ["admin", "admin_destructive"],
          "decision": "deny",
          "reason": "DDL not allowed on production database"
        }
      ]
    }
  ]
}
```

## ALTER TABLE Safety

### Safe Operations

**Adding nullable columns:**

```json
{
  "tool": "alter_table",
  "table": "users",
  "operations": [
    {
      "action": "add_column",
      "column": "middle_name",
      "definition": {
        "type": "varchar(50)",
        "nullable": true
      }
    }
  ]
}
```

**Classification:** `AdminSafe`

**Why safe:**
- No data migration required
- Existing rows get `NULL` for new column
- Reversible with `DROP COLUMN`

**Adding columns with defaults (PostgreSQL 11+):**

```json
{
  "operations": [
    {
      "action": "add_column",
      "column": "status",
      "definition": {
        "type": "varchar(20)",
        "nullable": false,
        "default": "'active'"
      }
    }
  ]
}
```

**Classification:** `AdminSafe`

**Why safe:**
- PostgreSQL 11+ stores default in metadata (no rewrite)
- Existing rows use default value
- Reversible with `DROP COLUMN`

**Adding validation constraints:**

```json
{
  "operations": [
    {
      "action": "add_constraint",
      "column": "age",
      "definition": {
        "constraint_type": "check",
        "expression": "age >= 0 AND age <= 150"
      }
    }
  ]
}
```

**Classification:** `AdminSafe`

**Why safe:**
- Validates new data only (unless `NOT VALID` flag used)
- Reversible with `DROP CONSTRAINT`
- No data loss

### Risky Operations

**Dropping columns:**

```json
{
  "operations": [
    {
      "action": "drop_column",
      "column": "deprecated_field"
    }
  ]
}
```

**Classification:** `Admin`

**Risks:**
- Irreversible data loss
- Application code may break
- Foreign keys may be orphaned

**Best practices:**
1. Preview with `preview_mutation`
2. Backup table before dropping
3. Update application code to stop using column
4. Test on staging environment first
5. Document reason for dropping column

**Renaming columns:**

```json
{
  "operations": [
    {
      "action": "rename_column",
      "column": "old_name",
      "definition": {
        "new_name": "new_name"
      }
    }
  ]
}
```

**Classification:** `Admin`

**Risks:**
- Application code breakage
- View/trigger/function references may break
- ORM metadata may be stale

**Best practices:**
1. Update all application code first
2. Use database views to alias old name temporarily
3. Test on staging environment
4. Deploy application and database changes together

**Changing column types:**

```json
{
  "operations": [
    {
      "action": "alter_column",
      "column": "price",
      "definition": {
        "type": "decimal(10,2)",
        "nullable": false
      }
    }
  ]
}
```

**Classification:** `Admin`

**Risks:**
- Data loss if type conversion fails
- Table rewrite (locks table on MySQL)
- Index rebuild required

**Best practices:**
1. Test type conversion on a copy: `SELECT CAST(price AS DECIMAL(10,2)) FROM products`
2. Check for data that doesn't fit new type
3. Add new column, migrate data, drop old column (safer than `ALTER COLUMN`)

### ALTER TABLE Checklist

Before running `alter_table`:

- [ ] **Preview** with `preview_mutation`
- [ ] **Classify** operation risk level
- [ ] **Backup** table if destructive
- [ ] **Test** on staging environment
- [ ] **Update** application code if needed
- [ ] **Check** foreign keys and constraints
- [ ] **Estimate** duration (large tables may lock)
- [ ] **Schedule** during maintenance window if needed
- [ ] **Document** reason for change
- [ ] **Monitor** execution and rollback plan

## CREATE/DROP Operations

### CREATE TABLE

**Basic table creation:**

```json
{
  "tool": "create_table",
  "table": "audit_log",
  "columns": [
    {
      "name": "id",
      "type": "serial",
      "primary_key": true
    },
    {
      "name": "user_id",
      "type": "integer",
      "nullable": false
    },
    {
      "name": "action",
      "type": "varchar(50)",
      "nullable": false
    },
    {
      "name": "created_at",
      "type": "timestamp",
      "nullable": false,
      "default": "CURRENT_TIMESTAMP"
    }
  ],
  "if_not_exists": true
}
```

**Classification:** `AdminSafe`

**Best practices:**
- Use `if_not_exists: true` for idempotency
- Define primary key explicitly
- Add `created_at`/`updated_at` columns
- Use appropriate data types (avoid `text` for everything)
- Add indexes for foreign keys

### DROP TABLE

**Dropping a table:**

```json
{
  "tool": "drop_table",
  "table": "legacy_table",
  "confirm": "legacy_table"
}
```

**Classification:** `AdminDestructive`

**Risks:**
- Irreversible data loss
- Foreign key constraints may be orphaned
- Application code may break
- Triggers and views may break

**Best practices:**
1. **Backup first:** `pg_dump -t legacy_table > backup.sql`
2. **Check dependencies:** Find foreign keys, views, triggers referencing table
3. **Preview:** Call `preview_mutation` to see what will be dropped
4. **Test on staging:** Run on staging environment first
5. **Confirm:** Require exact table name as confirmation (prevents typos)
6. **Document:** Record reason and restoration procedure

**Confirmation required:**

DBFlux requires exact table name in `confirm` parameter:

```json
{
  "table": "users",
  "confirm": "users"  // Must match exactly
}
```

If `confirm` does not match `table`, operation is rejected:

```json
{
  "error": "ConfirmationMismatch",
  "message": "Confirmation 'user' does not match table name 'users'"
}
```

### TRUNCATE TABLE

**Truncating a table:**

```json
{
  "tool": "truncate_table",
  "table": "session_cache",
  "confirm": "session_cache"
}
```

**Classification:** `AdminDestructive`

**Risks:**
- All data deleted (irreversible)
- Cannot be rolled back (even in transaction)
- Faster than `DELETE FROM table` but no row-level triggers

**Best practices:**
- Use `DELETE FROM table WHERE ...` if recovery needed
- Backup before truncating
- Confirm table name to prevent accidents

## Index Operations

### CREATE INDEX

**Creating an index:**

```json
{
  "tool": "create_index",
  "table": "users",
  "columns": ["email"],
  "unique": true,
  "if_not_exists": true
}
```

**Classification:** `AdminSafe`

**Best practices:**
- Use `if_not_exists: true` for idempotency
- Use `unique: true` for uniqueness constraints
- Use composite indexes for multi-column queries: `["last_name", "first_name"]`
- Use partial indexes for filtered queries (PostgreSQL): `WHERE active = true`

**PostgreSQL concurrent indexes:**

```json
{
  "tool": "create_index",
  "table": "large_table",
  "columns": ["status"],
  "concurrently": true
}
```

**Classification:** `AdminSafe`

**Why safe:**
- Does not lock table for reads or writes
- Can be used on production databases
- Takes longer but allows concurrent access

**Note:** Concurrent index creation cannot be run inside a transaction.

### DROP INDEX

**Dropping an index:**

```json
{
  "tool": "drop_index",
  "index_name": "idx_users_email",
  "if_exists": true
}
```

**Classification:** `Admin`

**Risks:**
- Query performance degradation
- Application queries may become slow

**Best practices:**
1. **Check usage:** Verify index is not used by queries
2. **Preview:** Call `preview_mutation` to see what will be dropped
3. **Test on staging:** Drop on staging and monitor query performance
4. **Keep DDL:** Save `CREATE INDEX` statement for restoration

## Testing DDL Changes

### Local Testing

1. **Use Docker containers:**

```bash
docker run -d --name test-postgres -e POSTGRES_PASSWORD=test postgres:15
```

2. **Apply DDL:**

```bash
./target/debug/dbflux mcp --client-id test
```

3. **Call `preview_mutation`:**

```json
{
  "tool": "alter_table",
  "connection_id": "test-postgres",
  "table": "users",
  "operations": [...]
}
```

4. **Verify schema:**

```json
{
  "tool": "describe_object",
  "connection_id": "test-postgres",
  "name": "users"
}
```

### Staging Environment Testing

1. **Clone production schema:**

```bash
pg_dump --schema-only prod > schema.sql
psql staging < schema.sql
```

2. **Apply DDL on staging:**

```json
{
  "tool": "alter_table",
  "connection_id": "staging-db",
  "table": "users",
  "operations": [...]
}
```

3. **Run application tests:**

```bash
npm test
cargo test
```

4. **Monitor performance:**

```sql
EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'test@example.com';
```

5. **Roll back if issues:**

```sql
BEGIN;
-- Test DDL
ROLLBACK;
```

### Integration Testing

DBFlux includes integration tests for DDL operations:

```rust
#[test]
fn test_alter_table_add_column_safe() {
    let req = AlterTableRequest {
        table: "users".to_string(),
        operations: vec![AlterOperation {
            action: AlterOperationAction::AddColumn,
            column: Some("phone".to_string()),
            definition: Some(ColumnDef {
                type_: "varchar(20)".to_string(),
                nullable: Some(true),
                ..Default::default()
            }),
        }],
    };

    let classification = classify_alter_table_operation(&req.operations[0]);
    assert_eq!(classification, ExecutionClassification::AdminSafe);
}
```

Run tests:

```bash
cargo test -p dbflux_policy test_alter_table
```

## Driver-Specific Behavior

### PostgreSQL

**DDL Transactions:**
- All DDL operations are transactional (can be rolled back)
- Exception: `CREATE INDEX CONCURRENTLY` cannot run in transaction

**ALTER TABLE behavior:**
- Adding columns with defaults is fast in PostgreSQL 11+ (metadata-only)
- Changing column types may require table rewrite (locks table)
- Dropping columns is fast (marks column as dropped, no rewrite)

**Best practices:**
- Use `CONCURRENTLY` for index operations on production
- Use `ALTER TYPE ADD VALUE` for enums (cannot be rolled back)
- Use `NOT VALID` for constraints (validate later)

### MySQL

**DDL Transactions:**
- DDL operations are NOT transactional (cannot be rolled back)
- `RENAME TABLE` is atomic

**ALTER TABLE behavior:**
- MySQL rewrites entire table for most `ALTER TABLE` operations
- Use `ALGORITHM=INPLACE` and `LOCK=NONE` for online DDL (MySQL 5.6+)
- `ADD COLUMN` at end of table is fast (no rewrite)

**Best practices:**
- Test DDL on copy of table first (cannot roll back)
- Use `ALGORITHM=INPLACE, LOCK=NONE` for large tables
- Use `pt-online-schema-change` for zero-downtime DDL

### SQLite

**DDL Transactions:**
- DDL operations are transactional (can be rolled back)

**ALTER TABLE behavior:**
- SQLite has very limited `ALTER TABLE` support
- Only `ADD COLUMN` and `RENAME COLUMN` supported
- `DROP COLUMN` and `ALTER COLUMN` require table recreation

**Best practices:**
- Use table recreation pattern for unsupported operations:
  1. `CREATE TABLE new_table (...)`
  2. `INSERT INTO new_table SELECT ... FROM old_table`
  3. `DROP TABLE old_table`
  4. `ALTER TABLE new_table RENAME TO old_table`

## Best Practices

### General Guidelines

1. **Always preview first:** Use `preview_mutation` before executing DDL
2. **Test on staging:** Never run untested DDL on production
3. **Backup before destructive changes:** Backup table before `DROP` or `TRUNCATE`
4. **Use transactions:** Wrap DDL in transactions when supported (PostgreSQL, SQLite)
5. **Schedule maintenance windows:** Run risky DDL during low-traffic periods
6. **Monitor execution:** Watch for locks, query performance, errors
7. **Document changes:** Record reason, rollback plan, and impact
8. **Update application code:** Deploy code and schema changes together
9. **Use idempotent DDL:** Use `IF NOT EXISTS` and `IF EXISTS` for safety
10. **Validate data first:** Check data quality before adding constraints

### AI Agent Guidelines

1. **Start with `describe_object`:** Understand schema before modifying
2. **Use `preview_mutation`:** Never execute DDL blind
3. **Check classification:** If `Admin` or `AdminDestructive`, explain risks to user
4. **Require confirmation:** Ask user to confirm destructive operations
5. **Suggest safer alternatives:** E.g., `ADD COLUMN` instead of `ALTER COLUMN`
6. **Explain tradeoffs:** Document why one approach is safer than another
7. **Test on copy:** Suggest testing on staging or local copy first
8. **Document rollback plan:** Explain how to undo the change
9. **Monitor approval queue:** Check if operation requires approval
10. **Respect policy decisions:** If denied, explain why and suggest alternatives

### Risk Assessment Checklist

For each DDL operation, ask:

- [ ] **What is the classification?** (AdminSafe, Admin, AdminDestructive)
- [ ] **Is it reversible?** (Can it be undone easily?)
- [ ] **What data is affected?** (How many rows, tables, columns?)
- [ ] **What are the dependencies?** (Foreign keys, views, triggers?)
- [ ] **How long will it take?** (Seconds, minutes, hours?)
- [ ] **Will it lock the table?** (Blocking reads or writes?)
- [ ] **What is the rollback plan?** (How to undo if it fails?)
- [ ] **Is there a safer alternative?** (Less risky approach?)
- [ ] **What is the user's intent?** (Why do they want this change?)
- [ ] **Is this the right environment?** (Staging vs production?)

## Error Recovery

### Handling Classification Errors

If classification fails (unknown operation):

```json
{
  "error": "UnknownOperation",
  "message": "Operation 'add_index' is not recognized",
  "valid_operations": ["add_column", "drop_column", "rename_column", "alter_column", "add_constraint", "drop_constraint"]
}
```

**Recovery:**
- Check operation name for typos
- Use `describe_object` to see current schema
- Refer to documentation for valid operations

### Handling Preview Errors

If preview fails (invalid syntax):

```json
{
  "error": "InvalidDefinition",
  "message": "Column type 'varchar' requires length: 'varchar(N)'",
  "column": "email"
}
```

**Recovery:**
- Fix definition syntax
- Use driver-specific type syntax (e.g., `varchar(255)` not `varchar`)
- Check column name for typos

### Handling Execution Errors

If DDL execution fails:

```json
{
  "error": "ConstraintViolation",
  "message": "CHECK constraint 'age_check' is violated by 5 rows",
  "constraint": "age_check",
  "violated_rows": 5
}
```

**Recovery:**
- Fix data that violates constraint
- Use `NOT VALID` constraint (PostgreSQL) and validate later
- Adjust constraint definition

### Handling Lock Timeouts

If DDL times out due to table lock:

```json
{
  "error": "LockTimeout",
  "message": "Could not acquire lock on table 'users' within 30s",
  "table": "users"
}
```

**Recovery:**
- Retry during low-traffic period
- Use concurrent index creation (PostgreSQL)
- Use online DDL (MySQL `ALGORITHM=INPLACE, LOCK=NONE`)
- Kill blocking queries (carefully!)

## Summary

DDL operations are powerful but risky. DBFlux provides multiple safety layers:

1. **Classification** — Categorize operations by risk
2. **Preview** — See what will happen before execution
3. **Governance** — Control who can execute DDL
4. **Approval** — Require human approval for risky operations
5. **Audit** — Log all DDL for accountability

**Key takeaways:**
- Always preview DDL with `preview_mutation` before executing
- Understand classification levels (AdminSafe, Admin, AdminDestructive)
- Test on staging environment first
- Backup before destructive operations
- Use idempotent DDL (`IF NOT EXISTS`, `IF EXISTS`)
- Document changes and rollback plans
- Respect policy decisions and approval requirements
- Know driver-specific behavior (PostgreSQL transactions, MySQL rewrites, SQLite limitations)

For more information, see:
- [WHERE Clause Syntax Guide](./WHERE_CLAUSE_SYNTAX.md)
- [MCP Server README](../README.md)
- [DBFlux Policy Documentation](../../dbflux_policy/README.md)
- [DBFlux Core Documentation](../../dbflux_core/README.md)
