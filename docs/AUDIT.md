# DBFlux Audit System

DBFlux logs all significant operations to a unified audit trail stored in SQLite. This covers query execution, connection lifecycle, hook execution, script runs, MCP governance decisions, and configuration changes.

## Storage Location

All audit events are stored in the unified database:

```
~/.local/share/dbflux/dbflux.db
```

Table: `aud_audit_events`

The same database stores all other runtime state (profiles, history, sessions). The schema is managed by the migration system in `dbflux_storage/src/migrations/`.

## Event Structure

Every audit event is an `EventRecord` (`dbflux_core/src/observability/types.rs`) with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `i64` | Auto-assigned on insert |
| `ts_ms` | `i64` | Unix timestamp in milliseconds |
| `level` | `EventSeverity` | `trace`, `debug`, `info`, `warn`, `error`, `fatal` |
| `category` | `EventCategory` | Domain of the event (see below) |
| `action` | `String` | Specific action identifier (e.g., `query_execute`) |
| `outcome` | `EventOutcome` | `success`, `failure`, `cancelled`, `pending` |
| `actor_type` | `EventActorType` | Who triggered the event |
| `actor_id` | `Option<String>` | Identity of the actor (MCP client ID, hook name, etc.) |
| `source_id` | `EventSourceId` | Where the event originated |
| `connection_id` | `Option<String>` | Connection profile ID |
| `database_name` | `Option<String>` | Target database name |
| `driver_id` | `Option<String>` | Driver ID (e.g., `postgres`, `mongodb`) |
| `object_type` | `Option<String>` | Type of object affected (e.g., `table`, `collection`) |
| `object_id` | `Option<String>` | ID/name of the specific object |
| `summary` | `String` | Human-readable description |
| `details_json` | `Option<String>` | Additional structured context as a JSON object |
| `error_code` | `Option<String>` | Error code on failure |
| `error_message` | `Option<String>` | Error message on failure |
| `duration_ms` | `Option<i64>` | Execution time in milliseconds |
| `session_id` | `Option<String>` | Session correlation ID |
| `correlation_id` | `Option<String>` | Cross-component correlation ID |

### Event Categories

| Category | String | What it captures |
|----------|--------|-----------------|
| `Query` | `query` | SQL execution, MongoDB queries, scan operations |
| `Connection` | `connection` | Connect, disconnect, reconnect lifecycle |
| `Hook` | `hook` | PreConnect, PostConnect, PreDisconnect, PostDisconnect |
| `Script` | `script` | Lua, Python, Bash script execution |
| `Mcp` | `mcp` | AI client tool calls and policy decisions |
| `Governance` | `governance` | Policy evaluation outcomes |
| `Config` | `config` | Profile changes, settings modifications |
| `System` | `system` | Application startup, panics, migrations |

### Actor Types

| Type | String | Meaning |
|------|--------|---------|
| `User` | `user` | Human operating the DBFlux GUI |
| `System` | `system` | Background system operation |
| `App` | `app` | Application acting autonomously |
| `McpClient` | `mcp_client` | AI agent via MCP protocol |
| `Hook` | `hook` | Lifecycle hook script |
| `Script` | `script` | User-authored script |

### Required Fields Per Category

Validation is enforced by `AuditService::validate_event()` before storage:

| Category | Required beyond `action` + `summary` |
|----------|--------------------------------------|
| `Query` | `connection_id`, `driver_id`, `duration_ms` (for execution events) |
| `Connection` | `connection_id` |
| `Hook` | `object_type`, `object_id`, `connection_id` |
| `Script` | `object_type`, `object_id` |
| `Mcp` | `actor_id`, `object_id` (tool name) |
| `Config` | `object_type`, `object_id` |
| `Governance`, `System` | No additional fields |

## Privacy and Redaction

By default, `AuditService` runs with these settings:

- **`redact_sensitive = true`**: Sensitive values (passwords, tokens, connection strings) in `details_json` and `error_message` are replaced with `[REDACTED]` before storage.
- **`capture_query_text = false`**: Full query text is never stored. Instead, a SHA256 fingerprint plus the original length are stored as `[FINGERPRINT:<16-char-hex>]` with `query_length`. This prevents sensitive data in queries from leaking into the audit log.
- **`max_detail_bytes = 65536`**: Payloads larger than 64 KiB are rejected to prevent storage bloat.

These can be changed at runtime via `AuditService::set_*()` methods. The MCP server exposes some of these via governance settings.

## Viewing Audit Events

### In the DBFlux UI

Navigate to **Workspace → Audit**. The unified audit view supports:

- Filtering by actor, tool/action, date range, decision, category
- Exporting filtered results to CSV or JSON

### Directly via SQLite

The database is a standard SQLite file. Query it directly:

```bash
sqlite3 ~/.local/share/dbflux/dbflux.db
```

Useful queries:

```sql
-- All events in the last 24 hours
SELECT id, datetime(ts_ms/1000, 'unixepoch') as ts, level, category, action, outcome, actor_id, summary
FROM aud_audit_events
WHERE ts_ms > (unixepoch('now') - 86400) * 1000
ORDER BY ts_ms DESC;

-- MCP tool calls only
SELECT id, datetime(ts_ms/1000, 'unixepoch') as ts, actor_id, object_id as tool, outcome, summary
FROM aud_audit_events
WHERE category = 'mcp'
ORDER BY ts_ms DESC;

-- All failed operations
SELECT id, datetime(ts_ms/1000, 'unixepoch') as ts, category, action, actor_id, error_message
FROM aud_audit_events
WHERE outcome = 'failure'
ORDER BY ts_ms DESC
LIMIT 50;

-- Query events by connection
SELECT id, datetime(ts_ms/1000, 'unixepoch') as ts, action, driver_id, duration_ms, summary
FROM aud_audit_events
WHERE category = 'query' AND connection_id = 'your-connection-id'
ORDER BY ts_ms DESC;

-- Events grouped by category and outcome
SELECT category, outcome, count(*) as count
FROM aud_audit_events
GROUP BY category, outcome
ORDER BY category, outcome;
```

### Via MCP Tools (AI clients)

The MCP tool surface exposes three audit tools (requires `admin` execution class):

```
query_audit_logs    — Filter events by actor, tool, date range, decision
get_audit_entry     — Retrieve a single event by ID
export_audit_logs   — Export filtered results as CSV or JSON
```

### Via Rust API

```rust
use dbflux_audit::{AuditService, AuditQueryFilter, AuditExportFormat};

let service = AuditService::new_sqlite_default()?;

// Query recent events
let filter = AuditQueryFilter {
    category: Some("mcp".to_string()),
    start_epoch_ms: Some(start_ms),
    limit: Some(100),
    ..Default::default()
};
let events = service.query(&filter)?;

// Export to CSV
let csv = service.export(&filter, AuditExportFormat::Csv)?;

// Export extended (all fields including details_json)
let json = service.export_extended(&filter, AuditExportFormat::Json)?;
```

## Generating Audit Events

### From Service Layers

Use the `EventSink` trait. All components that emit audit events accept an `Arc<dyn EventSink>`:

```rust
use dbflux_core::observability::{
    EventRecord, EventSink,
    types::{EventCategory, EventSeverity, EventOutcome, EventActorType, EventSourceId},
    actions,
};

// Build the event
let event = EventRecord::new(
    now_epoch_ms(),
    EventSeverity::Info,
    EventCategory::Query,
    EventOutcome::Success,
)
.with_typed_action(actions::QUERY_EXECUTE)
.with_summary("SELECT executed on users table")
.with_actor(EventActorType::User, None)
.with_source(EventSourceId::Local)
.with_connection("my-profile-id", Some("mydb"), Some("postgres"))
.with_object("table", "users")
.with_duration(42);

// Emit through the sink (injected via constructor or DI)
event_sink.record(event)?;
```

### Canonical Action Constants

Action strings are defined in `dbflux_core/src/observability/actions.rs`. Use constants rather than bare strings:

| Constant | String | Category |
|----------|--------|----------|
| `QUERY_EXECUTE` | `query_execute` | Query |
| `QUERY_EXECUTE_FAILED` | `query_execute_failed` | Query |
| `CONNECTION_CONNECT` | `connection_connect` | Connection |
| `CONNECTION_DISCONNECT` | `connection_disconnect` | Connection |
| `HOOK_PRE_CONNECT` | `hook_pre_connect` | Hook |
| `HOOK_POST_CONNECT` | `hook_post_connect` | Hook |
| `HOOK_PRE_DISCONNECT` | `hook_pre_disconnect` | Hook |
| `HOOK_POST_DISCONNECT` | `hook_post_disconnect` | Hook |
| `SCRIPT_EXECUTE` | `script_execute` | Script |
| `SCRIPT_EXECUTE_FAILED` | `script_execute_failed` | Script |
| `MCP_TOOL_CALL` | `mcp_tool_call` | Mcp |
| `MCP_TOOL_DENIED` | `mcp_tool_denied` | Mcp |
| `SYSTEM_PANIC` | `system_panic` | System |

### Required Fields Checklist

Before calling `record()`, ensure:

1. `action` is set and non-empty (use a constant from `actions`)
2. `summary` is set and non-empty (human-readable, one sentence)
3. Category-specific fields are present (see table above)
4. `details_json` is a valid JSON object if provided — not an array or primitive
5. `details_json` is under 64 KiB

### Failure Events

For failures, set outcome to `EventOutcome::Failure` and populate `error_code` and `error_message`:

```rust
let event = EventRecord::new(ts_ms, EventSeverity::Error, EventCategory::Query, EventOutcome::Failure)
    .with_typed_action(actions::QUERY_EXECUTE_FAILED)
    .with_summary("Query failed: syntax error")
    .with_connection("profile-id", Some("mydb"), Some("postgres"))
    .with_error("42601", "syntax error at or near \"SELEC\"");
```

`error_message` is redacted if it contains sensitive patterns. Use `error_code` for stable machine-readable error identifiers.

## Retention and Purge

Events can be purged by retention policy:

```rust
// Delete events older than 90 days, in batches of 500
let stats = service.purge_old_events(90, 500)?;
println!("Deleted {} events in {} batches", stats.deleted_count, stats.batches);
```

The purge is batched to avoid long write transactions. It is not run automatically — add it to a scheduled background task or operator runbook.

## Architecture

```
[Service layers]
  |  emit EventRecord via EventSink trait
  v
AuditService              (dbflux_audit/src/lib.rs)
  |  validate → fingerprint query text → redact sensitive values → enforce size limit
  v
SqliteAuditStore          (dbflux_audit/src/store/sqlite.rs)
  |  delegates to AuditRepository
  v
AuditRepository           (dbflux_storage/src/repositories/audit.rs)
  |  inserts into aud_audit_events
  v
~/.local/share/dbflux/dbflux.db
```

Key files:

| File | Role |
|------|------|
| `crates/dbflux_core/src/observability/types.rs` | `EventRecord`, all enum types |
| `crates/dbflux_core/src/observability/actions.rs` | Canonical action string constants |
| `crates/dbflux_audit/src/lib.rs` | `AuditService` — validate, preprocess, record |
| `crates/dbflux_audit/src/query.rs` | `AuditQueryFilter` |
| `crates/dbflux_audit/src/export.rs` | CSV/JSON export (basic and extended) |
| `crates/dbflux_audit/src/redaction.rs` | Sensitive value redaction logic |
| `crates/dbflux_audit/src/purge.rs` | Retention-based event purge |
| `crates/dbflux_audit/src/store/sqlite.rs` | SQLite store adapter |
| `crates/dbflux_storage/src/repositories/audit.rs` | `AuditRepository` + `AuditEventDto` |
