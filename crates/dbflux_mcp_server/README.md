# dbflux_mcp_server

Standalone MCP (Model Context Protocol) server for AI-driven database operations.

## Overview

`dbflux_mcp_server` provides a JSON-RPC interface over stdin/stdout for AI clients (Claude Desktop, OpenAI GPT, etc.) to interact with databases through DBFlux's governance layer.

**Key features:**
- **Unified database interface** — Query PostgreSQL, MySQL, SQLite, MongoDB, Redis, and DynamoDB with the same tools
- **Governance layer** — Policy engine, approval workflows, and audit logging
- **DDL safety system** — Preview, classify, and control schema changes
- **Driver-agnostic queries** — JSON WHERE clause syntax works across all drivers
- **Secure by default** — All operations classified and governed by policy

## Quick Start

### Running the Server

```bash
# Build and run
cargo build -p dbflux
./target/debug/dbflux mcp --client-id my-ai-client

# Or run directly
cargo run -p dbflux -- mcp --client-id my-ai-client
```

### Claude Desktop Integration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dbflux": {
      "command": "/path/to/dbflux",
      "args": ["mcp", "--client-id", "claude-desktop"]
    }
  }
}
```

Restart Claude Desktop and verify:

```
User: Can you connect to my databases?
Claude: I can see the following MCP tools available: connect, list_connections, select_data, ...
```

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                      AI Client (Claude, GPT)                 │
└─────────────────────────────────────────────────────────────┘
                            │ JSON-RPC (stdio)
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    dbflux_mcp_server                         │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              McpRuntime                             │    │
│  │  ┌─────────────────────────────────────────────┐    │    │
│  │  │         GovernanceService                   │    │    │
│  │  │  • PolicyEngine                             │    │    │
│  │  │  • ApprovalService                          │    │    │
│  │  │  • AuditService                             │    │    │
│  │  └─────────────────────────────────────────────┘    │    │
│  │  ┌─────────────────────────────────────────────┐    │    │
│  │  │         ToolCatalog                         │    │    │
│  │  │  • query tools (select_data, count, etc.)   │    │    │
│  │  │  • schema tools (describe, list, etc.)      │    │    │
│  │  │  • mutation tools (insert, update, delete)  │    │    │
│  │  │  • DDL tools (create, alter, drop)          │    │    │
│  │  │  • approval tools (approve, reject, list)   │    │    │
│  │  │  • audit tools (query_audit_logs)           │    │    │
│  │  └─────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Driver Layer (dbflux_core)                │
│  • PostgreSQL    • MongoDB    • Redis                        │
│  • MySQL         • DynamoDB                                  │
│  • SQLite                                                    │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                        Databases                             │
└─────────────────────────────────────────────────────────────┘
```

### Request Flow

1. **AI client sends JSON-RPC request** via stdin
2. **Server routes to tool handler** based on method name
3. **Authorization checks** client ID against trusted clients registry
4. **Policy evaluation** classifies operation and checks policy
5. **Approval check** (if required) queues for human approval
6. **Execution** delegates to driver layer
7. **Audit logging** records operation and result
8. **Response** returned to AI client via stdout

## Tool Catalog

### Connection Management

| Tool | Description | Classification |
|------|-------------|----------------|
| `list_connections` | List available database connections | Metadata |
| `connect` | Establish connection to database | Metadata |
| `disconnect` | Close database connection | Metadata |
| `get_connection_info` | Get connection metadata (version, status) | Metadata |

### Schema Discovery

| Tool | Description | Classification |
|------|-------------|----------------|
| `list_databases` | List databases on server | Metadata |
| `list_tables` | List tables in database | Metadata |
| `list_collections` | List collections (document databases) | Metadata |
| `describe_object` | Get table/collection schema | Metadata |
| `explain_query` | Get query execution plan | Metadata |

### Data Queries

| Tool | Description | Classification |
|------|-------------|----------------|
| `select_data` | Query data with WHERE clause | Read |
| `count_records` | Count records matching filter | Read |
| `aggregate_data` | Aggregate data (COUNT, SUM, AVG, MIN, MAX) | Read |

### Data Mutations

| Tool | Description | Classification |
|------|-------------|----------------|
| `insert_record` | Insert one or more records | Write |
| `update_records` | Update records matching filter | Write |
| `delete_records` | Delete records matching filter | Destructive |
| `upsert_record` | Insert or update on conflict | Write |

### DDL Operations

| Tool | Description | Classification |
|------|-------------|----------------|
| `create_table` | Create new table | AdminSafe |
| `alter_table` | Modify table schema | AdminSafe / Admin |
| `drop_table` | Delete table and data | AdminDestructive |
| `truncate_table` | Delete all records | AdminDestructive |
| `create_index` | Create index | AdminSafe |
| `drop_index` | Delete index | Admin |
| `preview_mutation` | Preview DDL/DML without execution | Metadata |

See [DDL Safety Guide](./docs/DDL_SAFETY.md) for detailed classification rules.

### Scripts

| Tool | Description | Classification |
|------|-------------|----------------|
| `list_scripts` | List saved scripts | Metadata |
| `get_script` | Get script content | Metadata |
| `create_script` | Create new script file | Write |
| `update_script` | Update script content | Write |
| `delete_script` | Delete script file | Destructive |
| `execute_script` | Execute script against connection | Read / Write / Destructive |

### Approval & Audit

| Tool | Description | Classification |
|------|-------------|----------------|
| `list_pending_executions` | List operations awaiting approval | Metadata |
| `get_pending_execution` | Get pending execution details | Metadata |
| `approve_execution` | Approve pending operation | Admin |
| `reject_execution` | Reject pending operation | Admin |
| `query_audit_logs` | Query audit log | Metadata |
| `get_audit_entry` | Get audit entry by ID | Metadata |
| `export_audit_logs` | Export audit logs (CSV/JSON) | Metadata |

## WHERE Clause Syntax

DBFlux uses a unified JSON WHERE clause syntax that works across all database drivers.

### Basic Examples

**Simple equality:**

```json
{
  "status": "active"
}
```

**Multiple conditions (implicit AND):**

```json
{
  "status": "active",
  "role": "admin"
}
```

**Comparison operators:**

```json
{
  "age": { "$gte": 18 },
  "score": { "$lt": 100 }
}
```

**Logical operators:**

```json
{
  "$or": [
    { "role": "admin" },
    { "role": "moderator" }
  ]
}
```

**Pattern matching:**

```json
{
  "email": { "$like": "%@example.com" }
}
```

**NULL handling:**

```json
{
  "deleted_at": null
}
```

**Array operations (PostgreSQL, MongoDB):**

```json
{
  "tags": { "$contains": "featured" }
}
```

### Complete Reference

See [WHERE Clause Syntax Guide](./docs/WHERE_CLAUSE_SYNTAX.md) for:
- All operators (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$like`, `$ilike`, `$regex`, `$contains`, `$overlap`, `$size`, `$all`)
- Logical composition (`$and`, `$or`, `$not`)
- Type coercion rules
- Driver-specific behavior
- Error handling
- Advanced examples

## DDL Preview System

Before executing schema changes, preview what will happen:

### Preview Request

```json
{
  "tool": "preview_mutation",
  "connection_id": "prod-db",
  "database": "public",
  "sql": "ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;"
}
```

### Preview Response

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

### Classification Levels

| Level | Description | Requires Approval? |
|-------|-------------|-------------------|
| `admin_safe` | Safe, reversible schema changes | No (default policy) |
| `admin` | Risky schema changes (rename, drop column) | Yes (default policy) |
| `admin_destructive` | Irreversible data loss (DROP TABLE) | Yes (always) |

See [DDL Safety Guide](./docs/DDL_SAFETY.md) for complete classification matrix and best practices.

## ALTER TABLE Safety

### Safe Operations (AdminSafe)

- `ADD COLUMN` (nullable or with default)
- `ADD CONSTRAINT` (CHECK, UNIQUE, FOREIGN KEY)
- `CREATE INDEX`

### Risky Operations (Admin)

- `DROP COLUMN` (irreversible data loss)
- `RENAME COLUMN` (application breakage)
- `ALTER COLUMN` (type change, may fail)
- `DROP CONSTRAINT`
- `DROP INDEX`

### Destructive Operations (AdminDestructive)

- `DROP TABLE`
- `TRUNCATE TABLE`
- `DROP DATABASE`

### Example: Safe Column Addition

```json
{
  "tool": "alter_table",
  "connection_id": "prod-db",
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

**Classification:** `AdminSafe` (nullable column, no data migration)

### Example: Risky Column Drop

```json
{
  "tool": "alter_table",
  "connection_id": "prod-db",
  "table": "users",
  "operations": [
    {
      "action": "drop_column",
      "column": "legacy_id"
    }
  ]
}
```

**Classification:** `Admin` (irreversible, may break foreign keys)

**Best practice:** Preview first with `preview_mutation`!

## Governance System

### Policy Engine

DBFlux evaluates every operation against a policy:

```yaml
roles:
  - name: ai_agent_default
    policies:
      - classification: [metadata, read]
        decision: allow
      - classification: [write]
        decision: allow
        max_rows: 1000
      - classification: [destructive]
        decision: require_approval
      - classification: [admin_safe]
        decision: allow
      - classification: [admin, admin_destructive]
        decision: require_approval
```

### Trusted Clients

Register AI clients to assign policies:

```json
{
  "trusted_clients": [
    {
      "client_id": "claude-desktop",
      "role": "ai_agent_default"
    },
    {
      "client_id": "trusted-agent",
      "role": "developer"
    }
  ]
}
```

### Approval Workflow

1. **Operation requires approval** (e.g., `DROP TABLE`)
2. **Queued in pending executions** (in-memory store)
3. **Human reviews** via `list_pending_executions`
4. **Approve or reject** via `approve_execution` / `reject_execution`
5. **Execution proceeds** (if approved) or fails (if rejected)

### Audit Logging

All operations are logged to SQLite (`~/.config/dbflux/audit.sqlite`):

```json
{
  "id": 123,
  "timestamp": "2024-03-24T10:30:00Z",
  "actor_id": "claude-desktop",
  "connection_id": "prod-db",
  "tool_id": "delete_records",
  "classification": "destructive",
  "decision": "approved",
  "parameters": { "table": "sessions", "where": { "expired": true } },
  "result": { "deleted_rows": 150 }
}
```

Query audit logs:

```json
{
  "tool": "query_audit_logs",
  "actor_id": "claude-desktop",
  "start_date": "2024-03-20T00:00:00Z",
  "end_date": "2024-03-24T23:59:59Z",
  "limit": 100
}
```

Export audit logs:

```json
{
  "tool": "export_audit_logs",
  "format": "csv",
  "decision": "approved"
}
```

## Configuration

### Connection Profiles

DBFlux reads connection profiles from `~/.config/dbflux/profiles.json`:

```json
{
  "profiles": [
    {
      "id": "prod-postgres",
      "name": "Production PostgreSQL",
      "driver": "postgres",
      "config": {
        "host": "db.example.com",
        "port": 5432,
        "database": "myapp",
        "username": "dbflux_user",
        "password": { "secret_ref": "prod-postgres-password" }
      }
    },
    {
      "id": "local-mongodb",
      "name": "Local MongoDB",
      "driver": "mongodb",
      "config": {
        "uri": "mongodb://localhost:27017",
        "database": "test"
      }
    }
  ]
}
```

### MCP Settings

MCP settings are stored in `~/.config/dbflux/mcp_settings.json`:

```json
{
  "trusted_clients": [
    {
      "client_id": "claude-desktop",
      "role": "ai_agent_default"
    }
  ],
  "roles": [
    {
      "name": "ai_agent_default",
      "policies": [
        {
          "classification": ["metadata", "read"],
          "decision": "allow"
        },
        {
          "classification": ["write"],
          "decision": "allow",
          "max_rows": 1000
        },
        {
          "classification": ["destructive", "admin", "admin_destructive"],
          "decision": "require_approval"
        }
      ]
    }
  ]
}
```

## Driver Support

### Relational Databases

| Driver | WHERE Clause | DDL | Transactions | Notes |
|--------|--------------|-----|--------------|-------|
| PostgreSQL | ✅ Full | ✅ Full | ✅ Yes | Full DDL transactions, JSONB support, array types |
| MySQL | ✅ Full | ✅ Full | ⚠️ Limited | DDL not transactional, use `ALGORITHM=INPLACE` for online DDL |
| SQLite | ✅ Full | ⚠️ Limited | ✅ Yes | Limited `ALTER TABLE` (only `ADD COLUMN`, `RENAME`) |

See driver READMEs:
- [PostgreSQL](../dbflux_driver_postgres/README.md)
- [MySQL](../dbflux_driver_mysql/README.md)
- [SQLite](../dbflux_driver_sqlite/README.md)

### Document Databases

| Driver | WHERE Clause | DDL | Notes |
|--------|--------------|-----|-------|
| MongoDB | ✅ Full | ⚠️ Limited | Translates to MongoDB query syntax; DDL is schema-less |

### Key-Value Databases

| Driver | WHERE Clause | DDL | Notes |
|--------|--------------|-----|-------|
| Redis | ⚠️ Pattern only | ❌ No | SCAN pattern matching on keys; no WHERE clause support |
| DynamoDB | ⚠️ FilterExpression | ⚠️ Limited | FilterExpression runs after scan (not indexed) |

## ColumnRef Pattern

DBFlux uses the `ColumnRef` type to represent column references in WHERE clauses:

```rust
pub enum ColumnRef {
    Name(String),                    // Simple column: "email"
    Nested(Vec<String>),             // Nested field: ["metadata", "profile", "age"]
    JsonPath { column: String, path: String },  // JSON path: { column: "config", path: "$.notifications.email" }
}
```

### Usage

**Simple column:**

```json
{
  "email": "user@example.com"
}
```

Parsed as: `ColumnRef::Name("email")`

**Nested field (MongoDB):**

```json
{
  "metadata.profile.age": { "$gte": 18 }
}
```

Parsed as: `ColumnRef::Nested(vec!["metadata", "profile", "age"])`

**JSON path (PostgreSQL JSONB):**

```json
{
  "config->notifications->email": true
}
```

Parsed as: `ColumnRef::JsonPath { column: "config", path: "$.notifications.email" }`

### Translation

- **SQL (PostgreSQL):** `(config->'notifications'->>'email')::boolean = true`
- **SQL (MySQL):** `JSON_EXTRACT(config, '$.notifications.email') = true`
- **MongoDB:** `{ "config.notifications.email": true }`

See `crates/dbflux_core/src/query/column_ref.rs` for implementation.

## Error Handling

### Common Errors

| Error | Cause | Recovery |
|-------|-------|----------|
| `PolicyDenied` | Policy engine denied request | Check trusted clients and role policies |
| `ApprovalRequired` | Operation requires human approval | Call `list_pending_executions` and wait for approval |
| `InvalidWhereClause` | WHERE clause syntax error | Fix JSON structure, check operator usage |
| `ColumnNotFound` | Column does not exist | Use `describe_object` to get column names |
| `TypeMismatch` | Type coercion failed | Fix value type or use explicit casting |
| `DriverNotFound` | Connection uses unknown driver | Check connection profile configuration |
| `ConnectionFailed` | Cannot connect to database | Check host, port, credentials, network |
| `QueryTimeout` | Query exceeded timeout | Add `limit`, optimize query, or increase timeout |
| `ConfirmationMismatch` | Destructive operation confirmation failed | Provide exact table/database name in `confirm` field |

### Error Response Format

```json
{
  "error": "ColumnNotFound",
  "message": "Column 'unknow_column' does not exist in table 'users'",
  "details": {
    "column": "unknow_column",
    "table": "users",
    "available_columns": ["id", "email", "name", "created_at"]
  }
}
```

## Best Practices for AI Agents

### Before Querying

1. **List connections:** `list_connections` to see available databases
2. **Connect:** `connect` to establish connection
3. **Discover schema:** `describe_object` to get column names and types
4. **Check indexes:** Review indexes for query optimization

### Before Mutating

1. **Preview:** Use `preview_mutation` for DDL operations
2. **Count first:** Use `count_records` to estimate impact
3. **Use WHERE clause:** Always filter mutations (never `DELETE FROM table` without WHERE)
4. **Confirm destructive ops:** Provide exact table name in `confirm` field

### Before Schema Changes

1. **Preview DDL:** Call `preview_mutation` to see SQL and classification
2. **Test on staging:** Suggest testing on staging environment first
3. **Backup first:** Recommend backup before destructive changes
4. **Check dependencies:** Review foreign keys, views, triggers
5. **Document changes:** Explain reason and rollback plan

### Policy Compliance

1. **Check trusted clients:** Verify client ID is registered
2. **Respect approvals:** If operation requires approval, explain to user
3. **Monitor audit log:** Suggest reviewing audit log for sensitive operations
4. **Use least privilege:** Prefer read-only operations when possible

### Performance

1. **Use limit and offset:** Paginate large result sets
2. **Filter on indexed columns:** Prefer indexed columns in WHERE clause
3. **Avoid leading wildcards:** `email LIKE '%@example.com'` cannot use index
4. **Batch mutations:** Use `insert_record` with multiple records instead of loops
5. **Use aggregations:** Prefer `count_records` over `SELECT COUNT(*)` for large tables

## Development

### Running Tests

```bash
# Unit tests
cargo test -p dbflux_mcp
cargo test -p dbflux_policy
cargo test -p dbflux_approval
cargo test -p dbflux_audit

# Integration tests (requires Docker)
cargo test -p dbflux_mcp_server --test integration
```

### Logging

Set `RUST_LOG` for debug output:

```bash
RUST_LOG=dbflux_mcp=debug ./target/debug/dbflux mcp --client-id test
```

### Debugging

Trace JSON-RPC messages:

```bash
RUST_LOG=dbflux_mcp=trace ./target/debug/dbflux mcp --client-id test
```

## Security Considerations

### Credentials

- Never log credentials or sensitive connection parameters
- Use secret references in connection profiles: `{ "secret_ref": "key-name" }`
- Store secrets in system keychain (macOS Keychain, GNOME Keyring, Windows Credential Manager)

### SQL Injection

- WHERE clauses are automatically parameterized
- Do not attempt to bypass parameterization
- Column names are validated against schema

### Access Control

- Use policy engine to restrict operations
- Register AI clients with appropriate roles
- Use connection-scoped policies for production databases
- Require approval for destructive operations

### Audit Trail

- All operations are logged to audit database
- Audit logs cannot be deleted via MCP tools
- Export audit logs regularly for compliance

## Limitations

### General

- **No subqueries:** WHERE clauses do not support subqueries
- **No joins in WHERE:** Use `select_data` with `joins` parameter
- **No computed columns:** Cannot reference virtual/computed columns
- **No database functions:** Limited function support in WHERE clauses

### Driver-Specific

- **SQLite:** No `DROP COLUMN` or `ALTER COLUMN` (requires table recreation)
- **MySQL:** DDL not transactional (cannot rollback)
- **MongoDB:** No SQL LIKE (use `$regex`)
- **Redis:** No WHERE clause support (key pattern only)
- **DynamoDB:** FilterExpression not indexed (runs after scan)

## Contributing

See [AGENTS.md](../../AGENTS.md) for development guidelines.

### Adding New Tools

1. Define tool schema in `crates/dbflux_mcp/src/tool_catalog.rs`
2. Implement handler in `crates/dbflux_mcp/src/handlers/`
3. Register in `McpRuntime::handle_tool_call()`
4. Add tests in `crates/dbflux_mcp/tests/`
5. Update this README

### Adding New Drivers

1. Implement `DbDriver` trait in new crate
2. Register in `crates/dbflux/src/app.rs`
3. Add WHERE clause translation
4. Add DDL support (optional)
5. Update driver README
6. Add to this README's driver support matrix

## Documentation

- [WHERE Clause Syntax Guide](./docs/WHERE_CLAUSE_SYNTAX.md) — Complete reference for filtering
- [DDL Safety Guide](./docs/DDL_SAFETY.md) — Classification, preview, and best practices
- [AGENTS.md](../../AGENTS.md) — Development guidelines and architecture
- [Policy Documentation](../dbflux_policy/README.md) — Policy engine reference
- [Audit Documentation](../dbflux_audit/README.md) — Audit logging reference

## License

See [LICENSE](../../LICENSE) file in repository root.
