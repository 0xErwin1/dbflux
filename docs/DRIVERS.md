# DBFlux Drivers

This document is a comparative overview of the database drivers shipped with
DBFlux. For per-driver details, follow the link to each driver crate's
`README.md`. For the internal driver architecture (traits, registration, the
`DbDriver`/`Connection` seam), see the **Driver System** section of
[`ARCHITECTURE.md`](../ARCHITECTURE.md). Contributors implementing a driver
should start with the [Driver Authoring Guide](DRIVER_AUTHORING.md).

## How drivers are abstracted

Every driver exposes a `DriverMetadata` value (defined in
`crates/dbflux_core/src/driver/capabilities.rs`). The UI is driver-agnostic and
adapts purely from this metadata. The relevant fields are:

- **`DatabaseCategory`** — selects the view model and terminology. Values:
  `Relational`, `Document`, `KeyValue`, `TimeSeries`, `LogStream`,
  `ObjectStorage`, `SecretStore`. A category lands together with a driver that declares it.
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
| Amazon Redshift | Relational | SQL | Multiple databases, schemas, views, SSH tunnel, SSL/client certificates, auth, query cancellation, prepared statements, pagination, sorting, filtering, CSV/JSON export | Read-only over the PostgreSQL wire protocol; single-statement; exposes Redshift storage hints; no writes/DDL, IAM/SSO, or indexes. |
| MySQL | Relational | SQL | Relational base + SSH tunnel, SSL, auth, foreign keys, check/unique constraints, routines, multi-statement | DDL is non-transactional; multi-statement scripts split text-based and run sequentially; routine listing covers FUNCTION/PROCEDURE only. |
| MariaDB | Relational | SQL | Same crate and capabilities as MySQL | Registered as a separate `mariadb` metadata sharing the MySQL implementation. |
| SQLite | Relational | SQL | Views, indexes, foreign keys, check/unique constraints, prepared statements, insert/update/delete, pagination, sorting, filtering, CSV/JSON export, query cancellation, transactional DDL, multi-statement | Embedded file driver: no network, SSH tunnel, or TLS; no multi-schema namespace. |
| SQL Server | Relational | SQL | Relational base + schemas, SSH tunnel, SSL, auth, foreign keys, check/unique constraints, transactional DDL, routines, multi-statement | Built on `tiberius`; named-instance lookup unavailable through SSH tunnel; multi-result-set batches return the last set as primary. |
| MongoDB | Document | MongoQuery | Document base + aggregation, SSH tunnel, indexes | MongoDB shell-style syntax only (no SQL); no query cancellation; parser scoped to supported command patterns. |
| Redis | Key-Value | RedisCommands | Key-value base + multiple databases, TTL, key types, value size, rename, bulk get, stream range/add/delete, auth, SSH tunnel, SSL | Redis command syntax only (no SQL); no query cancellation; SSH tunneling unavailable in URI mode. |
| DynamoDB | Document | Custom("DynamoDB") | Auth, pagination, filtering, insert/update/delete, nested documents, arrays | AWS-managed; native command envelope (`scan`/`query`/`put`/`update`/`delete`); no PartiQL/transactions; no query cancellation; `update many+upsert` unsupported. |
| CloudWatch Logs | Log Stream | Sql (metadata default) | Auth | AWS-managed; executes Logs Insights QL, OpenSearch PPL, and OpenSearch SQL via editor-managed source context; no query cancellation yet. |
| InfluxDB | Time Series | InfluxQuery | Auth, multiple databases, pagination, CSV/JSON export | v1 and v2 in one crate; InfluxQL on both, Flux on v2 only; read-only (no INSERT/UPDATE/DELETE); no transactions. |
| ClickHouse | Relational | SQL | Multiple databases, views, auth, pagination, sorting, filtering, grouping, joins, CTEs, windows, CSV/JSON export | HTTP(S), including ClickHouse Cloud; read-oriented DBFlux integration with no structured mutations, DDL, transactions, SSH tunneling, or query parameters. |
| TursoDB | Relational | SQL | Auth token, views, indexes, foreign keys, check/unique constraints, prepared statements, insert/update/delete, pagination, sorting, filtering, CSV/JSON export, transactions, transactional DDL, multi-statement | Remote Turso / libSQL over HTTP (`libsql://`, or `http://` for a local `sqld`); interactive transactions run on per-document server streams; no query cancellation, SSH tunnel, replicas, or database switching. |
| DuckDB | Relational | SQL | Multiple databases, schemas, views, indexes, foreign keys, check/unique constraints, prepared statements, insert/update/delete with RETURNING, pagination, sorting, filtering, CSV/JSON export, transactions, transactional DDL, query cancellation, row limits, read-only enforcement, multi-statement | Embedded file or in-memory database; attaches a DuckLake catalog on connect; only one process can hold a file for writing; no statement timeouts. |
| Amazon S3 | Object Storage | Custom("S3") | Auth (profile/SSO or static credentials, custom endpoint), bucket browsing, paginated object navigation, preview, full CRUD, presigned URLs | S3-compatible (Cloudflare R2, MinIO); no multipart upload/transfers panel, no embedded PDF viewer, no lifecycle/ACL management or S3 Select. |
| AWS Secrets | Secret Store | Custom("AWS Secrets") | Auth (profile/SSO or static credentials, custom endpoint), Secrets Manager and Parameter Store in one connection, paginated listing with name search, parameter path hierarchy, details, explicit value retrieval | Values can be edited and saved as new versions, version history can be browsed, and secrets can be created, rolled back, deleted, restored, rotated, tagged and described from the UI, each through the governed write path; one region per connection; LocalStack does not honor Parameter Store page sizes. |

## Per-driver summary

### PostgreSQL

Full SQL driver with schema discovery, stored routines (read-only viewer), SSL,
SSH tunneling, query cancellation via cancel tokens, transactional DDL, and
PostgreSQL-specific code generation. Multi-statement scripts run as a batch via
the simple query protocol. See
[`crates/dbflux_driver_postgres/README.md`](../crates/dbflux_driver_postgres/README.md).

### Amazon Redshift

Read-only relational SQL driver using the PostgreSQL wire protocol. It supports
schema, table, view, and column introspection; SSH tunneling; TLS and client
certificates; query cancellation; and Redshift distribution/sort-key storage
hints. It does not support writes or DDL, IAM/SSO authentication,
multi-statement queries, or indexes. See
[`crates/dbflux_driver_redshift/README.md`](../crates/dbflux_driver_redshift/README.md).

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

### ClickHouse

Relational SQL driver for self-hosted ClickHouse and ClickHouse Cloud over
HTTP(S). It discovers databases, tables, views, columns, and engine metadata,
and supports read-oriented SQL workflows with pagination and visual SELECT
generation. Structured mutations, DDL, transactions, SSH tunneling, and generic
query parameters are not supported in this initial scope. See
[`crates/dbflux_driver_clickhouse/README.md`](../crates/dbflux_driver_clickhouse/README.md).

### TursoDB

Relational SQL driver for Turso Cloud and self-hosted `sqld` over HTTP, built on
the `turso_serverless` SDK. It speaks the SQLite dialect, discovers tables,
views, columns, indexes, foreign keys, and constraints through `sqlite_master`
and PRAGMAs, and supports typed CRUD, bound parameters, batched scripts, and
interactive transactions on per-document server streams, one control
statement per editor run. Query cancellation, savepoints, SSH tunneling,
embedded replicas, and database switching are not supported. See
[`crates/dbflux_driver_turso/README.md`](../crates/dbflux_driver_turso/README.md).

### DuckDB

Embedded analytical SQL driver built on the bundled DuckDB engine. A profile
opens a database file, or an in-memory database when the path is empty, and can
attach a DuckLake catalog (a local metadata file or a PostgreSQL, SQLite, or
MySQL catalog, with data on local disk or object storage) that becomes the
default database. Every attached catalog is a database in the sidebar, with its
schemas, tables, and views loaded lazily. It supports bound parameters,
multi-statement scripts, row limits, cancellation, typed CRUD with `RETURNING`,
and read-only enforcement through a `READ ONLY` transaction that also refuses
file and network reads through table functions named in the statement; views
and macros created earlier, and scalar functions added by extensions, are not
checked. Statement timeouts are not supported. See
[`crates/dbflux_driver_duckdb/README.md`](../crates/dbflux_driver_duckdb/README.md).

### Amazon S3

Object-storage driver for AWS S3 and S3-compatible endpoints (Cloudflare R2,
MinIO), authenticating via AWS profile/SSO or static credentials with endpoint
override and path-style addressing. The connection root opens a buckets table;
bucket browsing paginates per level (AWS-console style) with an optional
non-paginated tree mode. Object preview covers images natively, text-like
objects in an inline editable buffer with save-back, and metadata plus
download/open-externally for PDF and other binary objects; archived storage
classes (GLACIER, DEEP_ARCHIVE) skip body preview entirely. Supports upload,
delete, type-to-confirm recursive prefix/bucket delete, folder/bucket
creation, rename (copy-then-delete), and presigned URLs. It does not support
multipart upload, a transfers panel, an embedded PDF viewer, lifecycle/ACL
management, or S3 Select. See
[`crates/dbflux_driver_s3/README.md`](../crates/dbflux_driver_s3/README.md).

### AWS Secrets

Secret-store driver for AWS Secrets Manager and Systems Manager Parameter
Store, authenticating via AWS profile/SSO or static credentials with an
optional endpoint override. One connection shows both stores in the sidebar.
Listing is paginated on the server side with name search, and Parameter Store
paths are browsed one level at a time. Listing and the details pane never
fetch a value: a value is read only when the user asks for it, with Retrieve value, with a version in the history, or with Edit value (the bottom bar's button or `e`) on a value that is not loaded yet, which retrieves it first; the row's right-click menu offers Retrieve value in that case, and Edit value once the value is loaded. Each of those is one retrieval, and it records an audit event without the value when auditing is enabled and the audit write succeeds; the retrieval itself can succeed without a record. Confirming Edit description on a Parameter Store parameter also reads its value again inside the driver, to write it back: that read is not shown and is not a retrieval. Key/value JSON secrets render as a
table, other text as plaintext and binary secrets as base64, and secrets owned
by another service are marked read only. A missing `kms:Decrypt` permission is
reported as an error, never as an empty value. The region picker lists the regions `ec2:DescribeRegions` returns; when the credentials cannot call it (an IAM user without that permission, LocalStack without EC2) the document quietly shows the connection's configured region without a picker, logs a warning for each failed discovery, and Refresh asks again. A value, including a JSON key/value secret, can be edited with Edit value (the details pane button, the row's right-click menu, or `e` on the selected row) and saved as a new version. Save goes through the governed write path: it refuses a read-only or approval-required connection and, while DBFlux's audit log is on, records one audit event that never carries the value; with auditing off, a save can succeed without a DBFlux audit record. If recording that event fails after the write landed, the write stands and the editor reports a warning, not a failure. A value stored as JSON opens Formatted, indented for editing, and can be switched to Raw. Save writes the text of the mode being edited: Raw saves the text verbatim, including any line breaks added to a value stored on one line, while Formatted puts a value stored on one line back on one line and writes a value stored on several lines as edited. In both modes Save refuses text that is no longer valid JSON. The editor shows line numbers, marks the lines that changed, and a footer with the size, the version, the number of changed lines and the caret. Binary values keep their read-only view. The details pane, its bottom bar and the row's right-click menu offer every other write the driver supports, every one of them except Restore (which only cancels a scheduled deletion and runs on one click) and the value editor's Save (a button, not a dialog) asking first in a dialog, nothing being written until it is confirmed, and each going through the same governed write path (a read-only or approval-required connection refuses it; while DBFlux's audit log is on, one audit event is recorded per attempt, and with auditing off, or when recording the event fails, the attempt has no DBFlux audit record, and a write that already landed stands and is reported as such; a failure is reported once with its correlation id): Roll back on every earlier version of a store with staging labels (`b` opens it for the most recent one), Delete (`x`) — scheduled with a recovery window of 7 to 30 days on Secrets Manager (the secret is unavailable from that moment until it is restored, and the window only limits how long Restore works) and never without recovery, irreversible on Parameter Store and only once the exact name is typed back — with Restore for a secret whose deletion is scheduled, Rotate now (its dialog says it runs the rotation function and creates a new version that becomes current when the rotation finishes) and Edit rotation, Edit tags, Edit description (which, on Parameter Store, writes the value again as a new version, and the dialog says so), and New secret or New parameter in the toolbar. A secret another service manages refuses these writes, and so does one whose deletion is scheduled, except Restore, which is the way back. A secret opens in its own tab from the details pane's expand button or the row menu's Open in tab, one tab per connection, store, name, version and region: it shows the same details, value, history, rotation and tags and offers the same writes through the same governed path, and its value stays masked until an audited Retrieve. A tab on a region other than the connection's, or on a secret that was deleted, starts no read or write and says why; a save already in flight is not cancelled and may still land on the previous region, which the tab then reports. Edit value in the details pane or the row menu opens that tab with the editor active and retrieves the value there first (audited) when it is not loaded; the side pane no longer hosts the editor, and you resize it by dragging its left edge. See
[`crates/dbflux_driver_aws_secrets/README.md`](../crates/dbflux_driver_aws_secrets/README.md).

## External RPC drivers

DBFlux can load drivers that run out-of-process and communicate over local IPC,
implemented through `dbflux_driver_ipc` and hosted via `dbflux_driver_host`.
These drivers register with the synthetic ID format `rpc:<socket_id>` and supply
their own `DriverMetadata` (category, query language, capabilities) over the
wire, so the UI treats them exactly like built-in drivers. For the discovery
handshake, service lifecycle, and protocol details, see
[`docs/DRIVER_RPC_PROTOCOL.md`](DRIVER_RPC_PROTOCOL.md).
