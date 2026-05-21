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

## Limitations

- SQL is not supported; queries must be written as Redis commands.
- Query cancellation is not supported (`QUERY_CANCELLATION` is not set); long-running commands cannot be aborted from the UI.
- No upsert (`supports_upsert: false`), no `RETURNING`, and no bulk update (`supports_bulk_update: false`).
- DDL capabilities are all disabled (no tables, views, indexes, schemas) — this is a key-value store, not relational.
- Transactions are advertised at the capability level (`supports_transactions: true`) but without isolation levels, savepoints, nested transactions, read-only, or deferrable support.
- Pub/Sub is not exposed (`PUBSUB` capability is not set).
- SSH tunneling is not available when URI mode is enabled; the tunnel path is wired only for manual connection mode.
- Stream consumer groups are not modeled; only range reads, entry add, and entry delete are supported.
