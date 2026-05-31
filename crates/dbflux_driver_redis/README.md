# dbflux_driver_redis

Redis key-value driver for DBFlux, built on the [`redis`](https://crates.io/crates/redis) crate.

## Features

- Key-value driver classified as `DatabaseCategory::KeyValue` with the `RedisCommands` query language; the editor uses Redis command syntax, not SQL.
- Connection modes: manual (host/port/user/password/database) and URI mode. URI mode accepts `redis://` and `rediss://` connection strings.
- Multiple logical databases via `SELECT <db>` (`MULTIPLE_DATABASES`). The active database index is tracked on the connection.
- Authentication with optional username + password (`AUTHENTICATION`).
- TLS/SSL with three modes (`off`, `on`, `verify`):
  - `off` — plain `redis://` connection.
  - `on` — `rediss://` with the certificate trusted without chain validation (insecure marker).
  - `verify` — `rediss://` with a supplied root certificate and optional client certificate/key, built through `Client::build_with_tls`.
- SSH tunnel support for reaching Redis through a bastion host (manual mode only; see Limitations).
- Key browsing and discovery:
  - Cursor-based key scanning (`KV_SCAN`, `PaginationStyle::Cursor`).
  - Per-key type discovery (`KV_KEY_TYPES`) across string, hash, list, set, sorted set, and stream.
  - TTL inspection (`KV_TTL`) and value size reporting (`KV_VALUE_SIZE`).
  - Existence checks (`KV_GET`/`KV_EXISTS`), key rename (`KV_RENAME`), and bulk get of multiple keys (`KV_BULK_GET`).
- Value type coverage: strings, hashes, lists, sets, sorted sets, and streams, including stream range reads, stream entry add, and stream entry delete (`KV_STREAM_RANGE`, `KV_STREAM_ADD`, `KV_STREAM_DELETE`).
- Configurable stream preview limit exposed as a connection setting.
- Mutations: insert, update, delete, batch operations, and bulk delete. The `RedisCommandGenerator` emits Redis commands for set/delete, hash set/delete, list push/set/remove, set add/remove, sorted-set add/remove, and stream add/delete, for use in previews and copy-as-command.
- JSON export of results (`EXPORT_JSON`).

### Instance Metrics

Exposes a curated set of live server metrics sourced from the `INFO` command output:

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
- SSH tunneling is not available when URI mode is enabled; the tunnel path is wired only for manual connection mode.
- Stream consumer groups are not modeled; only range reads, entry add, and entry delete are supported.
