# DBFlux Drivers

This document is a comparative overview of the database drivers shipped with
DBFlux. For per-driver details, follow the link to each driver crate's
`README.md`. For the internal driver architecture (traits, registration, the
`DbDriver`/`Connection` seam), see the **Driver System** section of
[`ARCHITECTURE.md`](../ARCHITECTURE.md).

## How drivers are abstracted

Every driver exposes a `DriverMetadata` value (defined in
`crates/dbflux_core/src/driver/capabilities.rs`). The UI is driver-agnostic and
adapts purely from this metadata. The relevant fields are:

- **`DatabaseCategory`** — selects the view model and terminology. Values:
  `Relational`, `Document`, `KeyValue`, `Graph`, `TimeSeries`, `WideColumn`,
  `LogStream`. (Not every value has a shipping driver.)
- **`QueryLanguage`** — drives editor mode, placeholder text, and query parsing.
  Values include `Sql`, `MongoQuery`, `RedisCommands`, `Cypher`, `InfluxQuery`,
  `Flux`, `Cql`, `CloudWatchLogsInsightsQl`, `OpenSearchPpl`, `OpenSearchSql`,
  the script languages `Lua` / `Python` / `Bash`, and `Custom(String)`.
- **`DriverCapabilities`** — a `u64` bitflag set declaring supported features
  (transactions, pagination, schemas, key-value operations, etc.). Convenience
  bases `RELATIONAL_BASE`, `DOCUMENT_BASE`, and `KEYVALUE_BASE` group the common
  flags for each category.

The capability flags listed below are exactly the ones each driver's
`DriverMetadata` sets in code; nothing is inferred.

## Comparison

| Driver | Category | Query language | Key capabilities | Notes / limitations |
| --- | --- | --- | --- | --- |
| PostgreSQL | Relational | SQL | Relational base + schemas, SSH tunnel, SSL, auth, foreign keys, check/unique constraints, custom types, `RETURNING`, transactional DDL, routines, multi-statement | Full SQL driver; routine viewer is read-only; transactional DDL except `CREATE INDEX CONCURRENTLY`. |
| MySQL | Relational | SQL | Relational base + SSH tunnel, SSL, auth, foreign keys, check/unique constraints, routines, multi-statement | DDL is non-transactional; multi-statement scripts split text-based and run sequentially; routine listing covers FUNCTION/PROCEDURE only. |
| MariaDB | Relational | SQL | Same crate and capabilities as MySQL | Registered as a separate `mariadb` metadata sharing the MySQL implementation. |
| SQLite | Relational | SQL | Views, indexes, foreign keys, check/unique constraints, prepared statements, insert/update/delete, pagination, sorting, filtering, CSV/JSON export, query cancellation, transactional DDL, multi-statement | Embedded file driver: no network, SSH tunnel, or TLS; no multi-schema namespace. |
| SQL Server | Relational | SQL | Relational base + schemas, SSH tunnel, SSL, auth, foreign keys, check/unique constraints, transactional DDL, routines, multi-statement | Built on `tiberius`; named-instance lookup unavailable through SSH tunnel; multi-result-set batches return the last set as primary. |
| MongoDB | Document | MongoQuery | Document base + aggregation, SSH tunnel, indexes | MongoDB shell-style syntax only (no SQL); no query cancellation; parser scoped to supported command patterns. |
| Redis | Key-Value | RedisCommands | Key-value base + multiple databases, TTL, key types, value size, rename, bulk get, stream range/add/delete, auth, SSH tunnel, SSL | Redis command syntax only (no SQL); no query cancellation; SSH tunneling unavailable in URI mode. |
| DynamoDB | Document | Custom("DynamoDB") | Auth, pagination, filtering, insert/update/delete, nested documents, arrays | AWS-managed; native command envelope (`scan`/`query`/`put`/`update`/`delete`); no PartiQL/transactions; no query cancellation; `update many+upsert` unsupported. |
| CloudWatch Logs | Log Stream | Sql (metadata default) | Auth | AWS-managed; executes Logs Insights QL, OpenSearch PPL, and OpenSearch SQL via editor-managed source context; no query cancellation yet. |
| InfluxDB | Time Series | InfluxQuery | Auth, multiple databases, pagination, CSV/JSON export | v1 and v2 in one crate; InfluxQL on both, Flux on v2 only; read-only (no INSERT/UPDATE/DELETE); no transactions. |

## Per-driver summary

### PostgreSQL

Full SQL driver with schema discovery, stored routines (read-only viewer), SSL,
SSH tunneling, query cancellation via cancel tokens, transactional DDL, and
PostgreSQL-specific code generation. Multi-statement scripts run as a batch via
the simple query protocol. See
[`crates/dbflux_driver_postgres/README.md`](../crates/dbflux_driver_postgres/README.md).

### MySQL / MariaDB

One crate implements both MySQL and MariaDB. Supports SQL execution, schema
discovery, query cancellation via `KILL QUERY`, code generation, and routine
discovery for functions and procedures. DDL is not transactional and
multi-statement splitting is text-based. See
[`crates/dbflux_driver_mysql/README.md`](../crates/dbflux_driver_mysql/README.md).

### SQLite

Embedded, file-based driver with schema discovery, query cancellation via
interrupt handles, transactional DDL, and code generation. No network transport,
SSH tunneling, or TLS, and no multi-schema namespace. See
[`crates/dbflux_driver_sqlite/README.md`](../crates/dbflux_driver_sqlite/README.md).

### SQL Server

Built on the `tiberius` TDS client. Supports SQL Server / Azure SQL, TLS modes,
named instances (resolved via SQL Browser), SSH tunneling, per-tab database
switching, and multi-result-set batches. See
[`crates/dbflux_driver_mssql/README.md`](../crates/dbflux_driver_mssql/README.md).

### MongoDB

Document driver with collection browsing, document CRUD, MongoDB shell-style
query parsing, aggregation, and document-focused schema metadata. SQL is not
supported and query cancellation is unavailable. See
[`crates/dbflux_driver_mongodb/README.md`](../crates/dbflux_driver_mongodb/README.md).

### Redis

Key-value driver covering strings, hashes, lists, sets, sorted sets, and
streams, plus key scanning, TTL operations, rename, bulk get, and multiple
logical databases. SQL is not supported and SSH tunneling is unavailable in URI
mode. See
[`crates/dbflux_driver_redis/README.md`](../crates/dbflux_driver_redis/README.md).

### DynamoDB

AWS NoSQL driver built on `aws-sdk-dynamodb` with region/profile/endpoint
configuration. Table discovery maps PK/SK and GSI/LSI metadata; execution uses a
native command envelope (`scan`, `query`, `put`, `update`, `delete`). PartiQL and
DynamoDB transactions are not exposed. See
[`crates/dbflux_driver_dynamodb/README.md`](../crates/dbflux_driver_dynamodb/README.md).

### CloudWatch Logs

AWS CloudWatch Logs driver executing queries through `StartQuery` with
editor-managed time range and log-group source context. Query documents can run
Logs Insights QL, OpenSearch PPL, and OpenSearch SQL; schema discovery enumerates
log groups and exposes log streams as event-stream children. Its
`DriverMetadata.query_language` is set to `Sql` as the default editor mode while
the actual mode is chosen per query document. See
[`crates/dbflux_driver_cloudwatch/README.md`](../crates/dbflux_driver_cloudwatch/README.md).

### InfluxDB

Time-series driver supporting both InfluxDB v1 and v2 in one crate. InfluxQL runs
on both versions; Flux runs on v2 only. The query API is read-only (no
INSERT/UPDATE/DELETE, no transactions), with optional default bucket/database and
per-query bucket routing. See
[`crates/dbflux_driver_influxdb/README.md`](../crates/dbflux_driver_influxdb/README.md).

## External RPC drivers

DBFlux can load drivers that run out-of-process and communicate over local IPC,
implemented through `dbflux_driver_ipc` and hosted via `dbflux_driver_host`.
These drivers register with the synthetic ID format `rpc:<socket_id>` and supply
their own `DriverMetadata` (category, query language, capabilities) over the
wire, so the UI treats them exactly like built-in drivers. For the discovery
handshake, service lifecycle, and protocol details, see
[`docs/DRIVER_RPC_PROTOCOL.md`](DRIVER_RPC_PROTOCOL.md).
