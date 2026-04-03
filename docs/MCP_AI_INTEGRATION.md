# DBFlux AI + MCP Integration Guide

This guide explains how to integrate AI agents with DBFlux via the standalone MCP server binary.

It is intentionally explicit about what is available today and what is still pending, so integrations do not rely on behavior that is not implemented.

## 1. Architecture Overview

DBFlux exposes MCP server functionality via the `dbflux mcp` subcommand that speaks the Model Context Protocol over stdio. AI clients (Claude Desktop, Cursor, etc.) launch this binary as a subprocess and communicate via JSON-RPC 2.0, newline-delimited.

```
AI Client (Claude Desktop / Cursor / any MCP client)
        |  stdio  (JSON-RPC 2.0, newline-delimited)
        v
  dbflux mcp                    ← integrated into main dbflux binary
        |
        +--  dbflux_mcp          governance, authorization, tool catalog
        +--  dbflux_core         profiles, config, driver traits
        +--  dbflux_driver_*     real database drivers
        +--  dbflux_policy       policy engine
        +--  dbflux_audit        audit trail (SQLite)
```

The MCP server and the DBFlux GUI app are independent processes. They share the same on-disk config files under `~/.config/dbflux/` and the same profile store. Governance configured in the GUI (trusted clients, roles, policies, per-connection settings) is read by the server on startup.

## 2. Running the MCP Server

### Build

```bash
# All drivers with MCP support (default)
cargo build -p dbflux --release

# SQLite only with MCP
cargo build -p dbflux --features sqlite,mcp --release

# Without MCP support (AI integration disabled)
cargo build -p dbflux --no-default-features --features sqlite,postgres,mysql,mongodb,redis,dynamodb,lua,aws --release
```

The MCP server is integrated into the main `dbflux` binary.

### Usage

```
dbflux mcp --client-id <id> [--config-dir <path>]
```

| Flag | Description |
|------|-------------|
| `--client-id <id>` | Identity of this AI client. Must match a registered trusted client in governance settings. **Required.** |
| `--config-dir <path>` | Override the default config directory (`~/.config/dbflux`). Useful for isolated test environments. |

### Claude Desktop config

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or the equivalent on your platform:

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

The `client-id` value must match a trusted client entry you created in the DBFlux GUI under **Settings → MCP → Clients**.

**Note**: If you built DBFlux without the `mcp` feature (`--no-default-features`), the MCP server will not be available.

## 3. Governance Model (Core Concepts)

Every AI request is enforced through all of these layers in order:

1. **Trusted client**: requester identity must be active and registered.
2. **Connection MCP gate**: target connection must have MCP enabled.
3. **Policy assignment**: actor must have a scoped assignment on that connection.
4. **Tool + classification allowlist**: both the tool ID and its execution class must be permitted by the assigned policy.
5. **Approval path**: write/destructive flows can require human approval before execution.
6. **Audit trail**: every decision is appended to `aud_audit_events` in the unified SQLite database and is queryable/exportable. See `docs/AUDIT.md` for the full event schema.

All six layers run inside the server process on every `tools/call` request. None can be bypassed from the client side.

## 4. Canonical Tool Surface (v1)

| Group | Tool ID | What it does |
|-------|---------|--------------|
| Discovery | `list_connections` | Enumerate all configured database connections |
| Discovery | `get_connection` | Retrieve details of a specific connection |
| Discovery | `get_connection_metadata` | Fetch driver capabilities and metadata |
| Schema | `list_databases` | List all databases accessible on a connection |
| Schema | `list_schemas` | List schemas within a database |
| Schema | `list_tables` | List tables and views within a schema |
| Schema | `list_collections` | List MongoDB collections |
| Schema | `describe_object` | Get column/field definitions and indexes for a table |
| Query | `read_query` | Execute a SELECT or equivalent read-only query |
| Query | `explain_query` | Show the query execution plan without executing the target mutation |
| Query | `preview_mutation` | Return a read-only preview/plan for a write query; the mutation is never executed |
| Scripts | `list_scripts` | List saved scripts in the scripts directory |
| Scripts | `get_script` | Retrieve the source of a specific saved script |
| Scripts | `create_script` | Save a new script to the scripts directory |
| Scripts | `update_script` | Overwrite an existing saved script |
| Scripts | `delete_script` | Permanently remove a script |
| Scripts | `run_script` | Execute a saved script against a connection |
| Approval | `request_execution` | Submit a mutation for human approval before it runs |
| Approval | `list_pending_executions` | View all executions awaiting approval |
| Approval | `get_pending_execution` | Retrieve details of a specific pending execution |
| Approval | `approve_execution` | Approve a pending mutation (admin only) |
| Approval | `reject_execution` | Reject and discard a pending mutation (admin only) |
| Audit | `query_audit_logs` | Search and filter the MCP audit trail |
| Audit | `get_audit_entry` | Retrieve a single audit log entry by ID |
| Audit | `export_audit_logs` | Download audit log entries as CSV or JSON |

Deferred tools (explicitly rejected at request time in v1):

- `estimate_query_cost`
- `get_execution_status`

Not exposed in this branch:

- `preview_ddl` — DBFlux intentionally does not expose schema preview until it has a safe, non-mutating implementation across drivers

## 5. Execution Classes

Policies gate tools at two levels: the tool ID itself and the execution classification. A request is allowed only when both match the policy's allowlist.

| Class | What it covers |
|-------|---------------|
| `metadata` | Schema inspection — listing databases, tables, and describing objects |
| `read` | Running read-only queries, fetching data, and read-only previews |
| `write` | Inserting, updating, or running scripts that modify data |
| `destructive` | DELETE, DROP, TRUNCATE and other irreversible operations |
| `admin_safe` | Safe DDL operations such as additive schema changes and index creation |
| `admin` | Risky DDL operations, approvals, audit export, and privileged actions |
| `admin_destructive` | Irreversible admin operations such as dropping or truncating schema objects |

## 6. Built-in Policies and Roles

Three policies and three roles are shipped as immutable built-ins. They are always present regardless of what is persisted on disk, and cannot be deleted or modified.

### Built-in policies

| ID | Allowed classes | Scope |
|----|----------------|-------|
| `builtin/read-only` | metadata, read | All discovery + schema tools; read-only query and preview tools; script listing/get; audit read tools |
| `builtin/write` | metadata, read, write | All read-only tools plus write-capable script and request/approval-submission flows |
| `builtin/admin` | metadata, read, write, destructive, admin_safe, admin, admin_destructive | All canonical tools exposed in this branch |

### Built-in roles

| ID | Assigned policy |
|----|----------------|
| `builtin/read-only` | `builtin/read-only` |
| `builtin/write` | `builtin/write` |
| `builtin/admin` | `builtin/admin` |

Built-ins are injected at startup in both the GUI app (`AppState`) and the MCP server (`bootstrap::init`). They are never written to disk. Any attempt to delete a built-in returns an error.

For most integrations, assign `builtin/read-only` to start and escalate to `builtin/write` or a custom policy only when write access is explicitly needed.

## 7. Operator Setup in DBFlux GUI

Configure governance in the DBFlux GUI before starting the MCP server.

1. **Settings → MCP → Clients tab**
   - Register each AI agent as a trusted client (stable `client_id`, human-readable name, optional issuer).
   - Mark clients active. Inactive clients are denied at the first authorization gate.

2. **Settings → MCP → Roles tab**
   - Built-in roles (`Read Only`, `Write`, `Admin`) appear at the top and cannot be deleted.
   - Create custom roles by combining multiple policies using the multi-select dropdown.

3. **Settings → MCP → Policies tab**
   - Built-in policies appear at the top and cannot be modified.
   - Create custom policies by toggling tool and class checkboxes.

4. **Connection Manager → MCP tab**
   - Enable MCP for the target connection.
   - Select the actor (trusted client), role, and/or policy for this connection from populated dropdowns.

5. **Workspace → Pending Approvals**
   - Review and approve/reject write/destructive requests that triggered the approval path.

6. **Workspace → Audit**
   - Filter by actor/tool/decision/time range and export CSV/JSON.

The MCP server reads these settings from disk on startup. If you change governance settings in the GUI while the server is running, restart the server to pick up the new config.

## 8. Persisted Files and Paths

DBFlux persists all state in a single unified SQLite database and a few supporting directories. Paths are resolved by `dirs` (`XDG_*` on Linux, `~/Library` on macOS).

Typical Linux defaults:

| Path | Contents |
|------|----------|
| `~/.local/share/dbflux/dbflux.db` | Unified database: profiles, auth, SSH tunnels, governance, audit events, history, sessions, UI state |
| `~/.local/share/dbflux/sessions/` | Scratch and shadow files for auto-save session restore |
| `~/.local/share/dbflux/scripts/` | User-authored scripts directory |

The `dbflux.db` database contains all domain tables under prefixed schemas:

- `cfg_*` — config (profiles, auth, governance, services, hooks, drivers)
- `st_*` — state (sessions, query history, UI state, saved queries)
- `aud_audit_events` — unified audit log (MCP events, query events, connections, hooks, scripts)
- `sys_*` — system (migrations, legacy import tracking)

Built-in policies and roles are synthesized at startup and never written to disk.

Important for tests: do not use real user directories. Pass `--config-dir` to the binary or set `HOME`/`XDG_CONFIG_HOME`/`XDG_DATA_HOME` to temp paths for isolated runs. The `dbflux_audit::temp_sqlite_path(name)` helper generates isolated paths for audit tests.

## 9. Rust Integration Pattern

### In-process (GUI app, `AppState`)

```rust
// Register a trusted client
state.upsert_mcp_trusted_client(TrustedClientDto {
    id: "agent-a".into(),
    name: "Agent A".into(),
    issuer: None,
    active: true,
})?;

// Assign a built-in role to the agent on a connection
state.save_mcp_connection_policy_assignment(ConnectionPolicyAssignmentDto {
    connection_id: connection_id.to_string(),
    assignments: vec![ConnectionPolicyAssignment {
        actor_id: "agent-a".into(),
        role_ids: vec!["builtin/read-only".into()],
        policy_ids: vec![],
    }],
})?;
```

### Checking built-in IDs before deletion

```rust
if dbflux_mcp::is_builtin(id) {
    // built-ins cannot be modified or deleted
}
```

### Authorization call (used internally by the MCP server)

```rust
use dbflux_mcp::server::authorization::{AuthorizationRequest, authorize_request};

let outcome = authorize_request(
    &trusted_clients,
    &policy_engine,
    &audit_service,
    &AuthorizationRequest {
        identity: RequestIdentity { client_id: "agent-a".into(), issuer: None },
        connection_id: connection_id.to_string(),
        tool_id: "read_query".to_string(),
        classification: ExecutionClassification::Read,
        mcp_enabled_for_connection: true,
    },
    now_epoch_ms(),
)?;

if !outcome.allowed {
    // deny_code and deny_reason explain why
}
```

## 10. Integration Checklist

Before pointing an AI client at the MCP server:

- [ ] `dbflux` built with MCP support (enabled by default, or with `--features mcp`)
- [ ] Trusted client registered and active in DBFlux GUI
- [ ] `--client-id` passed to the binary matches the registered client
- [ ] Target connection has MCP enabled
- [ ] Actor has a policy assignment on that connection
- [ ] Policy covers the tools the agent will use
- [ ] Approval workflow understood for any write/destructive tools

## 11. Test Hygiene

To avoid polluting developer machines during tests:

- Pass `--config-dir` to a temp directory or set `HOME`/`XDG_CONFIG_HOME`/`XDG_DATA_HOME`.
- Use temp SQLite paths for audit tests.
- Do not read/write `~/.config/dbflux` or `~/.local/share/dbflux` in test code.
- Built-in policies and roles are available without any setup — do not insert them manually in test fixtures.
- The `dbflux_audit::temp_sqlite_path(name)` helper generates an isolated path for each test.

## 12. Troubleshooting

### Server exits immediately

- Missing `--client-id` argument.
- Config directory is inaccessible or cannot be created.

### Request denied as untrusted

- Verify the client exists and is active in trusted clients list.
- Verify `--client-id` exactly matches the registered `id` (case-sensitive).

### Request denied as connection not MCP-enabled

- Enable MCP in the target connection's governance settings (Connection Manager → MCP tab).
- Or set `mcp_enabled_by_default: true` in the config if you want all connections enabled.

### Policy denied

- Confirm the actor has an assignment on that connection scope.
- Confirm the tool ID is in the assigned policy's allowed tools.
- Confirm the execution class is in the policy's allowed classes.
- If using `builtin/read-only`, write tools (`preview_mutation`, `create_script`, etc.) are excluded by design.

### Approval stuck pending

- Check the pending queue in the DBFlux workspace and approve/reject explicitly.
- `approve_execution` requires the `admin` class — ensure the approver's policy includes it.

### Audit export missing events

- Verify filters (`actor_id`, `tool_id`, time range, decision) are not over-restrictive.
- `export_audit_logs` requires the `admin` execution class.

### Cannot delete policy or role

- Built-in IDs (`builtin/read-only`, `builtin/write`, `builtin/admin`) cannot be deleted.
- Create a custom policy with a different ID if you need a modifiable variant.

### Settings changed in GUI but server still uses old values

- Restart the MCP server process. Governance is loaded from disk once at startup.
