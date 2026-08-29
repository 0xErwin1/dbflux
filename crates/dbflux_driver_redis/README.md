# Redis

In-memory key-value database.

## At a glance

- **Category** — Key-value
- **Query language** — Redis commands
- **Default port** — 6379
- **URI scheme** — `redis`

Redis key-value driver for DBFlux, built on the [`redis`](https://crates.io/crates/redis) crate.

## Features

- Key-value driver classified as `DatabaseCategory::KeyValue` with the `RedisCommands` query language; the editor uses Redis command syntax, not SQL.
- Connection modes: manual (host/port/user/password/database) and URI mode. URI mode accepts `redis://` and `rediss://` connection strings.
- Multiple logical databases via `SELECT <db>` (`MULTIPLE_DATABASES`). The active database index is tracked on the connection.
- Authentication with optional username + password (`AUTHENTICATION`).
- Reports its client identity to the server via `CLIENT SETNAME` on connect (`dbflux/<version>`, visible in `CLIENT LIST`); best-effort, since some managed providers restrict `CLIENT` commands.
- TLS/SSL with three modes (`off`, `on`, `verify`):
  - `off` — plain `redis://` connection.
  - `on` — `rediss://` with the certificate trusted without chain validation (insecure marker).
  - `verify` — `rediss://` with a supplied root certificate and optional client certificate/key, built through `Client::build_with_tls`.
- SSH tunnel support for reaching Redis through a bastion host (manual mode only; see Limitations).
- Deployment topology can be detected automatically or set explicitly (`standalone`, `cluster`, `sentinel`):
  - Automatic detection probes `ROLE` and `INFO cluster` at connect time and routes to standalone or Cluster handling.
  - `cluster` skips detection and connects directly via `ClusterClient`, using the primary host/port plus any configured additional seed nodes. A Cluster connection only ever exposes database 0; a nonzero database on a Cluster profile is rejected at connect time instead of being silently applied to db 0.
  - `sentinel` connects through `SentinelClient`, resolving the named master from one or more Sentinel nodes (primary host/port plus any configured additional nodes). After resolving, the driver runs `CLIENT SETNAME`, `PING`, and a `ROLE` sanity check that the resolved node is actually a master.
  - Sentinel failover recovery: a connection-class failure (dropped connection, IO error) on a Sentinel-backed connection triggers exactly one re-resolve through Sentinel and one retry of the failed command before the error is surfaced.
- Key browsing and discovery:
  - Cursor-based key scanning (`KV_SCAN`, `PaginationStyle::Cursor`). On a Cluster connection, a plain `SCAN` has no single-node meaning, so the driver fans it out across every master: the page budget is split evenly across masters still pending, each node's cursor is tracked independently, and the aggregated cursor round-trips as an opaque JSON object mapping `"<host>:<port>"` to its pending `SCAN` cursor. The overall scan is exhausted once every master reports cursor 0.
  - Per-key type discovery (`KV_KEY_TYPES`) across string, hash, list, set, sorted set, and stream.
  - TTL inspection (`KV_TTL`) and value size reporting (`KV_VALUE_SIZE`).
  - Existence checks (`KV_GET`/`KV_EXISTS`), key rename (`KV_RENAME`), and bulk get of multiple keys (`KV_BULK_GET`).
- Value type coverage: strings, hashes, lists, sets, sorted sets, and streams, including stream range reads, stream entry add, and stream entry delete (`KV_STREAM_RANGE`, `KV_STREAM_ADD`, `KV_STREAM_DELETE`).
- Configurable stream preview limit exposed as a connection setting.
- Mutations: insert, update, delete, batch operations, and bulk delete. The `RedisCommandGenerator` emits Redis commands for set/delete, hash set/delete, list push/set/remove, set add/remove, sorted-set add/remove, and stream add/delete, for use in previews and copy-as-command.
- JSON export of results (`EXPORT_JSON`).
- Size gate on whole-payload reads: when the request carries a byte budget, string/JSON values are probed with `STRLEN` before `GET` and oversized values return a placeholder with the real size instead of transferring the payload; collection types are unaffected, and stream reads that hit the fetch cap report themselves as truncated.

Schema introspection reports a single aggregated `db0` keyspace on a Cluster connection: the key count and average TTL are summed/averaged across every master's `DBSIZE`/keyspace stats rather than reported per-node.

### Instance Metrics

Exposes a curated set of live server metrics sourced from the `INFO` command output. Not available on a Cluster connection — there is no single node to sample `INFO` against, so `instance_catalog()` returns `None` and Instance Overview/metrics/inspectors are unavailable for Cluster profiles.

- `redis.connected_clients` — currently connected clients
- `redis.blocked_clients` — clients waiting on a blocking command
- `redis.used_memory` — bytes allocated by Redis allocator
- `redis.used_memory_rss` — bytes allocated by the OS (resident set size)
- `redis.total_commands_processed` — cumulative commands processed
- `redis.total_connections_received` — cumulative connections accepted
- `redis.instantaneous_ops_per_sec` — commands processed per second (server-side rate)
- `redis.keyspace_hits` — cache hits against key lookups
- `redis.keyspace_misses` — cache misses against key lookups
- `redis.evicted_keys` — keys evicted due to `maxmemory` policy
- `redis.expired_keys` — keys expired by TTL
- `redis.rdb_changes_since_last_save` — changes since last RDB snapshot
- `redis.connected_slaves` — attached replica count

Each metric is returned as a single `(timestamp_ms, value)` row for live charting.

### Instance Inspector

Exposes tabular snapshots of running server state:

- `redis.client_list` — active clients from `CLIENT LIST` (id, cmd, age, idle, flags, db, sub, multi)

Sensitive fields (`addr`, `laddr`, `name`) are redacted to `[redacted]` to avoid exposing client IP addresses and hostnames.

## Limitations

- SQL is not supported; queries must be written as Redis commands.

- Instance metrics return a single data point per call (current snapshot from `INFO`), not a historical time series. Cumulative counters (e.g. `redis.total_commands_processed`) grow monotonically — interpret them as deltas between samples rather than absolute rates.

- The `CLIENT LIST` inspector redacts the `addr`, `laddr`, and `name` fields in every row to avoid exposing client IP addresses and user-supplied names to the UI.

- Query cancellation is not supported (`QUERY_CANCELLATION` is not set); long-running commands cannot be aborted from the UI.
- No upsert (`supports_upsert: false`), no `RETURNING`, and no bulk update (`supports_bulk_update: false`).
- DDL capabilities are all disabled (no tables, views, indexes, schemas) — this is a key-value store, not relational.
- Transactions are advertised at the capability level (`supports_transactions: true`) but without isolation levels, savepoints, nested transactions, read-only, or deferrable support.
- Pub/Sub is not exposed (`PUBSUB` capability is not set).
- SSH tunneling is not available when URI mode is enabled; the tunnel path is wired only for manual connection mode. Combining an SSH tunnel with Cluster or Sentinel additional seed nodes is not supported: the tunnel forwards only the primary host/port, so additional nodes are unreachable through it.
- Stream consumer groups are not modeled; only range reads, entry add, and entry delete are supported.
- Sentinel and Cluster additional seed nodes are always contacted over plain `redis://`; per-node TLS configuration for those extra nodes is not supported. The resolved Sentinel master connection itself is also plain (no TLS) in this iteration.
- Sentinel authentication applies only to the resolved master connection (via the configured username/password); the Sentinel nodes themselves are contacted without authentication.
