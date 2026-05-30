# dbflux_driver_mongodb

MongoDB document driver for DBFlux.

## Features

- Document driver classified as `DatabaseCategory::Document` with the `MongoQuery` query language; the editor uses MongoDB shell syntax, not SQL.
- Connection modes: manual (host/port/credentials/database) and URI mode. URI mode accepts `mongodb://` and `mongodb+srv://` connection strings (SRV records are parsed for replica-set discovery).
- Multiple logical databases (`MULTIPLE_DATABASES`) with collection browsing and document counting.
- Authentication (`AUTHENTICATION`) and TLS/SSL with three modes (`off`, `on`, `verify`), supporting a root certificate and optional client certificate.
- SSH tunnel support for reaching MongoDB through a bastion host.
- Shell-style query parsing for `db.collection.method(...)` and `db.method(...)` forms, with a JSON-document fallback for backward compatibility. Supported methods: `find`, `findOne`, `aggregate`, `count`/`countDocuments`, `insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`. Parse errors carry byte-offset positions for editor diagnostics.
- Aggregation pipelines (`AGGREGATION`); query capabilities advertise order-by, group-by, having, limit, and offset.
- WHERE operators: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `NotIn`, and the logical `And`/`Or`/`Not`.
- Pagination via cursor and page-token styles (`PaginationStyle::Cursor`, `PaginationStyle::PageToken`).
- Document-focused schema metadata: collection fields and indexes (`INDEXES`), with nested documents and arrays mapped into the document-tree view (`NESTED_DOCUMENTS`, `ARRAYS`).
- Mutations: insert, update (including upsert), and delete (`supports_upsert: true`). The `MongoShellGenerator` emits `insertOne`/`insertMany`, `updateOne`/`updateMany` (with `{ upsert: true }`), and `deleteOne`/`deleteMany` for previews and copy-as-query.
- DDL: drop database, drop collection, create index, and drop index.
- JSON export of results (`EXPORT_JSON`).

### Instance Metrics

Exposes a curated set of live server metrics sourced from the MongoDB `serverStatus` command. Metrics are extracted via BSON dotted-path traversal:

- `mongo.connections_current` — current open connections
- `mongo.connections_available` — available connection slots
- `mongo.opcounters_insert` — insert operations since startup
- `mongo.opcounters_query` — query operations since startup
- `mongo.opcounters_update` — update operations since startup
- `mongo.opcounters_delete` — delete operations since startup
- `mongo.opcounters_getmore` — getMore operations since startup
- `mongo.mem_resident` — resident memory in MB
- `mongo.mem_virtual` — virtual memory in MB
- `mongo.network_bytes_in` — bytes received since startup

Each metric is returned as a single `(timestamp_ms, value)` row for live charting.

### Instance Inspector

Exposes tabular snapshots of running server state:

- `mongo.current_op` — in-progress operations from `$currentOp` aggregation pipeline (opid, type, ns, op, secs_running, wait_for_lock)

## Limitations

- SQL is not supported; queries must use MongoDB shell-style syntax (or the JSON fallback).

- Instance metrics return a single data point per call (current snapshot from `serverStatus`), not a historical time series. Operations counters (e.g. `mongo.opcounters_insert`) grow monotonically — interpret them as deltas between samples rather than absolute rates.

- `$currentOp` requires the `inprog` privilege or `clusterMonitor` role on Atlas clusters. Without sufficient privileges, `fetch_inspector_snapshot("mongo.current_op")` returns an empty result set.
- Query cancellation is not supported (`QUERY_CANCELLATION` is not set).
- `RETURNING` is not supported; mutation capabilities also report no batch, no bulk update, and no bulk delete at the capability level (`supports_batch`, `supports_bulk_update`, `supports_bulk_delete` are all `false`), even though the generator can emit `updateMany`/`deleteMany` text.
- Parser coverage is intentionally scoped to the supported method set above, not the full interactive shell language; `distinct` is not surfaced as a query capability (`supports_distinct: false`).
- No joins, subqueries, unions, CTEs, window functions, or `EXPLAIN` at the query-capability level.
- Transactions are advertised at the capability level (`supports_transactions: true`) but without isolation levels, savepoints, nested transactions, read-only, or deferrable support.
- DDL is not transactional (`transactional_ddl: false`); create-database, create-collection, alter, views, and triggers are not supported.
