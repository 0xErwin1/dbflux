# Key Concepts

This guide is the short mental model for contributors and advanced users.
It describes the contracts between subsystems; [Architecture](../ARCHITECTURE.md) remains the
canonical, exhaustive map of crate boundaries and key files.

## Mental model

```text
UI document
  -> app orchestration (profiles, connections, policy, lifecycle)
    -> dbflux_core contracts (metadata, capabilities, requests, values)
      -> built-in driver or RPC-adapted driver
        -> QueryResult -> generic result views
        -> EventRecord -> audit sink
```

The important direction is inward: presentation and workflows depend on contracts, while
driver-specific behavior stays behind those contracts. Audit observes work across the flow rather
than forming a separate execution path.

## Concept map

| Concept | What it is | Why it matters | Go deeper |
|---|---|---|---|
| Drivers | Implementations of the `DbDriver` and `Connection` contracts. | Built-in and external databases enter the app through the same boundary. | [Core traits](../crates/dbflux_core/src/core/traits.rs), [Driver Authoring](DRIVER_AUTHORING.md) |
| `DriverMetadata` | A driver's declarative identity, category, language, forms, and detailed feature descriptors. | Generic workflows can select presentation and behavior without identifying a concrete driver. | [Metadata definition](../crates/dbflux_core/src/driver/capabilities.rs) |
| `DriverCapabilities` | Feature flags declared by a driver and exposed by a connection. | The UI enables only supported operations instead of guessing from database names. | [Capability flags](../crates/dbflux_core/src/driver/capabilities.rs) |
| Documents | Type-erased panes managed as tabs, with identity and event contracts. | New document types can participate without extending a closed document enum. | [`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs), [`TabManager`](../crates/dbflux_ui_document/src/tab_manager.rs) |
| Query results | Structured shapes, columns, rows, and values returned by connections. | Generic tables, trees, text views, exports, and charts consume one result model. | [Result types](../crates/dbflux_core/src/query/types.rs), [`Value`](../crates/dbflux_core/src/core/value.rs) |
| MCP governance | Trusted-client, connection, classification, policy, approval, and audit enforcement around AI tools. | Agent access is explicit, scoped, reviewable, and observable. | [AI + MCP Integration](MCP_AI_INTEGRATION.md) |
| Audit | The cross-cutting `EventRecord`/`EventSink` observability seam. | Queries, lifecycle work, hooks, governance, and external services share a correlated trail. | [Audit reference](AUDIT.md), [`EventSink`](../crates/dbflux_core/src/observability/source.rs) |
| Hooks | Commands, scripts, or Lua attached to connection lifecycle phases. | Environment setup and cleanup remain outside driver implementations, with explicit failure behavior. | [Hook contracts](../crates/dbflux_core/src/connection/hook.rs), [Settings & Hooks](SETTINGS.md#connection-hooks) |
| RPC services | Persisted descriptors adapted at startup into drivers or auth providers. | Out-of-process integrations join the runtime without becoming UI special cases. | [RPC config](RPC_SERVICES_CONFIG.md), [protocol](DRIVER_RPC_PROTOCOL.md) |

## Drivers are contracts, not UI cases

`DbDriver` creates and describes a database integration; `Connection` exposes operations on an
active connection. Their current contracts live in
[`core/traits.rs`](../crates/dbflux_core/src/core/traits.rs). Built-in crates implement them directly,
while external drivers are adapted over RPC.

The decoupling rule is strict: UI and app workflow code adapt through generic metadata,
capabilities, and contracts, never concrete driver IDs. If a feature requires
`if driver == "postgres"` in presentation or workflow code, the missing abstraction belongs in
metadata, a capability, or a core contract.

### Metadata and capabilities

[`DriverMetadata`](../crates/dbflux_core/src/driver/capabilities.rs) describes what a driver is:
display identity, `DatabaseCategory`, query language, syntax and operation descriptors, limits, and
other generic presentation inputs. `DriverCapabilities` declares which broad features it supports.
A connection exposes the same metadata and capabilities so callers do not need its originating
driver object.

Use metadata to choose a generic mode and capabilities to gate an operation. Do not infer support
from a driver key, icon, native type-name string, or a list maintained in the UI.

## Documents are open polymorphism

[`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs) is the document polymorphism seam. It
type-erases each concrete GPUI entity behind closures for rendering, focus, commands, metadata,
lifecycle behavior, deduplication, and subscription. The workspace therefore does not model
documents as a closed enum of concrete document types.

[`DocumentKey`](../crates/dbflux_ui_document/src/dedup.rs) expresses open-document identity. Each
pane decides whether it matches a key, and
[`TabManager`](../crates/dbflux_ui_document/src/tab_manager.rs) uses that contract to focus an
existing tab instead of opening a duplicate.

Documents emit [`DocumentEvent`](../crates/dbflux_ui_document/src/handle.rs). The tab manager and
workspace translate those events into cross-document actions without reaching into a concrete
document implementation. Add a pane behavior at this seam rather than adding concrete-type
matching to workspace code.

## Query results are structured data

The current result boundary is [`QueryResult`](../crates/dbflux_core/src/query/types.rs): a declared
`QueryResultShape`, `ColumnMeta` entries, rows of core `Value`, optional text or bytes, execution
timing, and possible additional result sets. [`Value`](../crates/dbflux_core/src/core/value.rs)
preserves relational and document values without reducing everything to JSON or display strings.

Structured result values and column metadata feed generic views. In particular, `ColumnMeta.kind`
carries semantic type information such as timestamp, float, integer, text, or unknown. Charts and
other consumers use that semantic kind; they must not sniff `ColumnMeta.type_name` or branch on
driver identity.

## MCP governance wraps execution

The MCP process authorizes requests through trusted-client identity, the per-connection MCP gate, execution classification, assigned roles and policies, and approval where required. Decisions and executions are audited. See the [governance model](MCP_AI_INTEGRATION.md#3-governance-model-core-concepts), [`dbflux_mcp` authorization](../crates/dbflux_mcp/src/server/authorization.rs), the [policy engine](../crates/dbflux_policy/src/engine.rs), and the [approval service](../crates/dbflux_approval/src/service.rs).

Safety properties are part of the boundary:

- `preview_mutation` is read-governed and produces a read-only plan; it does not execute the mutation. The implementation rejects any driver-generated preview query that is not metadata/read classified ([query tool](../crates/dbflux_mcp_server/src/tools/query.rs)).
- `select_data` currently rejects requested joins instead of silently ignoring them ([read tool](../crates/dbflux_mcp_server/src/tools/read.rs)).
- Mutation preview is not a DDL-preview surface. DDL operations are separate governed tools; no DDL preview tool is exposed in the current [tool catalog](../crates/dbflux_mcp/src/tool_catalog.rs).

Keep classification, policy, approval, and audit decisions at the governance boundary. A handler must not weaken them because an underlying driver can perform the operation.

## Audit is the observability seam

Services emit canonical [`EventRecord`](../crates/dbflux_core/src/observability/types.rs) values through [`EventSink`](../crates/dbflux_core/src/observability/source.rs). The record carries actor, source, category, outcome, target context, details, and correlation fields; the sink owns validation and storage behavior.

This makes audit cross-cutting: query execution, connection lifecycle, hooks, MCP decisions, configuration, and external RPC services can be observed without coupling their domain logic to the SQLite implementation. Use the [Audit reference](AUDIT.md) for schema, validation, redaction, retention, and tracing-bridge details.

## Hooks surround connection lifecycle

[`ConnectionHook`](../crates/dbflux_core/src/connection/hook.rs) defines command, script, or Lua work at `PreConnect`, `PostConnect`, `PreDisconnect`, and `PostDisconnect`. Hooks belong to orchestration around a connection, not to the database driver's query contract.

Failure policy is explicit: `Disconnect` aborts the phase, `Warn` continues with a surfaced warning, and `Ignore` continues while logging the failure. Execution can be blocking or detached, with timeout, environment, and ready-signal controls. See [Settings & Connection Hooks](SETTINGS.md#connection-hooks) for configuration and safety details.

## RPC services are runtime descriptors

RPC services are persisted launch and compatibility descriptors classified as `Driver` or `AuthProvider`. At startup, [`dbflux_app::rpc_services`](../crates/dbflux_app/src/rpc_services/) discovers descriptors, validates and probes the appropriate protocol, then adapts successful services into the driver or auth-provider registry. One service family failing does not redefine the other.

External driver registry keys remain `rpc:<socket_id>`. Auth providers use their provider identity and never appear as database drivers. Runtime metadata for an external driver comes from its handshake, not from UI conditionals. See [RPC Services Config](RPC_SERVICES_CONFIG.md) for persistence and [Driver RPC Protocol](DRIVER_RPC_PROTOCOL.md) for transport, negotiation, lifecycle, and audit emission.

## Where to make a change

| You need to change… | Start at… | Recognition rule |
|---|---|---|
| A database operation available to every integration | [`DbDriver`/`Connection`](../crates/dbflux_core/src/core/traits.rs) | Define a generic contract before implementing drivers. |
| Whether a generic feature is shown or allowed | [Metadata and capabilities](../crates/dbflux_core/src/driver/capabilities.rs) | Declare support; never identify the driver in UI/workflow code. |
| Result rendering or semantic type behavior | [Query result types](../crates/dbflux_core/src/query/types.rs) | Consume shape, values, and `ColumnMeta.kind`. |
| A new workspace document | [`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs) and [`DocumentEvent`](../crates/dbflux_ui_document/src/handle.rs) | Implement the open pane seam and a dedup key; do not extend a concrete document union. |
| An AI tool or execution rule | [MCP integration](MCP_AI_INTEGRATION.md) and [authorization](../crates/dbflux_mcp/src/server/authorization.rs) | Preserve classification, policy, approval, and audit ordering. |
| An observable domain action | [`EventRecord` and `EventSink`](../crates/dbflux_core/src/observability/types.rs) | Emit through the seam; keep storage details out of domain code. |
| Connection setup or cleanup automation | [Hook contract](../crates/dbflux_core/src/connection/hook.rs) | Choose a lifecycle phase and explicit failure mode. |
| An out-of-process driver or auth provider | [RPC services](RPC_SERVICES_CONFIG.md) | Persist a descriptor and adapt through `dbflux_app::rpc_services`. |

Continue with [Architecture](../ARCHITECTURE.md) for canonical crate boundaries and key files, or [Driver Authoring](DRIVER_AUTHORING.md) to implement a built-in or external driver.
