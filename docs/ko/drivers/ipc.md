# External RPC drivers

Bridge that lets a driver running in its own process serve DBFlux over a local
socket, so a database DBFlux does not ship support for can be added without
forking the application.

## At a glance

- **Category** — declared by the remote driver
- **Query language** — declared by the remote driver
- **Registration id** — `rpc:<socket_id>`
- **Protocol** — see the driver RPC protocol reference

## How it works

The bridge holds no knowledge of any particular database. On connect it performs a
`Hello` handshake with the remote process, and everything DBFlux needs to present
the driver — its kind, its metadata, its connection form — comes back in that
reply. From then on the bridge forwards operations over the socket and translates
the responses into the same core contracts a built-in Rust driver implements, so
the rest of the application cannot tell the difference.

Connection values are persisted as `DbConfig::External { kind, values }`, keyed by
the form the remote driver declared.

### Capabilities negotiated at handshake

The remote driver advertises what it supports, and the bridge adapts:

- `SchemaIntrospection` — the sidebar builds a schema tree from the driver
- `MultiDatabase` — the connection exposes more than one database
- `ChunkedResults` — large result sets stream in chunks rather than one response
- `Cancellation` — a running query can be cancelled from the UI
- `AuditEmit` — the driver may emit its own audit events (protocol v1.2 and later)

Anything not advertised is simply absent from the interface, the same way a
built-in driver's unset capability flags remove features from the UI.

### Host lifecycle

A configured RPC service may be managed by DBFlux: the host process is spawned on
demand, waited on until it reports healthy, and tracked for shutdown. A service
without launch configuration is expected to be running already.

### Audit forwarding

Drivers that advertise `AuditEmit` can send `EmitAuditEvent` frames interleaved
with their responses. The bridge intercepts those frames and dispatches them to
the host's sanitizing sink, so an external driver's events land in the same audit
log as everything else, with the same redaction applied. Frames from a driver that
did not advertise the capability are discarded rather than trusted.

## Limitations

- Requires a compatible driver host process and a reachable socket. The bridge
  cannot start a host that has no launch configuration, so an unavailable service
  stays unavailable.
- The effective feature set is bounded by the remote driver's advertised metadata
  and by its implementation. The bridge never adds capability the driver does not
  have.
- Audit emission needs protocol v1.2 or later and `AuditEmit` in the handshake.
  Older drivers emit nothing, and their operations appear in the log only through
  the events DBFlux records on their behalf.
