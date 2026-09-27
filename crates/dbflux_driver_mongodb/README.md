# MongoDB

Document database for modern applications.

## At a glance

- **Category** — Document
- **Query language** — MongoDB query syntax
- **Default port** — 27017
- **URI scheme** — `mongodb`

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
- **Collection browsing in the data grid (`DocumentFeatures::QUERY_SLOTS`)**: collection browses accept a projection and a sort document next to the filter, and `sample_collection_schema` reads a random sample (`$sample`, after the filter's `$match`) to report each field path's presence, type distribution (`String`, `Int32`, `Decimal128`, `Object`, `Array`, …) and a value summary. The grid uses the sample for field-path completion in the query bar, the presence bars in the column headers and the Schema view. Counts without a filter come from `estimatedDocumentCount` and are labelled as estimates.
- **Field edits (`DocumentFeatures::FIELD_PATCH`)**: `patch_document` sends `updateOne` with `$set` / `$unset` on the changed paths only and keeps BSON types (decimals stay `Decimal128`, dates stay `Date`, object ids stay `ObjectId`, and text is never read as an ObjectId). `replace_document` sends `replaceOne` without touching `_id`, and `fetch_document` reads one document by `_id` so the grid can tell whether it changed after the page loaded. The shell generator renders the exact write for the server-change confirmation, for example `db.products.updateOne({ _id: ObjectId("…") }, { $set: { "price.amount": Decimal128("119.00") } })`.
- **Aggregate view (`DocumentFeatures::AGGREGATE`)**: `aggregate_collection` runs a pipeline of JSON stages against the collection, with `allowDiskUse` off unless the request asks for it. The driver appends a `$limit` one past the requested cap and marks the result truncated when the pipeline yields more; a pipeline that ends in `$out` or `$merge` keeps that stage last and returns no documents. Results come back in the same document shape `browse_collection` uses. The shell generator renders the pipeline as `db.<collection>.aggregate([...])`, and the language service flags a pipeline with an `$out` or `$merge` stage as `MongoAggregateWrite`, so it goes through the dangerous-query confirmation before it runs.
- **Visual query builder (`DocumentFeatures::VISUAL_BUILDER`)**: `Connection::document_query_codec()` returns a codec that renders a `DocumentQuerySpec` as the `filter` / `project` / `sort` / `limit` slots, as an aggregation pipeline (`$match`, `$group` with `$count` / `$sum` / `$avg`, `$sort`, `$skip`, `$limit`) and as shell preview text, and reads the slots back into a spec. Clauses the spec cannot hold, such as `$expr`, come back as unrepresentable text instead of being dropped, so the builder shows a sync conflict rather than rewriting them. See [Document collections](../../docs/QUERY_BUILDER.md#document-collections).
- **Extended JSON dates**: `{"$date": "<RFC 3339>"}` in the query slots, in aggregation pipelines and in document writes is decoded as a BSON `Date`. An invalid date string there fails with an error instead of being stored as a subdocument.
- Mutations: insert, update (including upsert), and delete (`supports_upsert: true`). The `MongoShellGenerator` emits `insertOne`/`insertMany`, `updateOne`/`updateMany` (with `{ upsert: true }`), and `deleteOne`/`deleteMany` for previews and copy-as-query.
- DDL: drop database, drop collection, create index, and drop index.
- JSON export of results (`EXPORT_JSON`).
- Reports client identity as `appName=dbflux/<version>` on connect (visible in server logs and `db.currentOp()`), unless the connection URI already sets an `appName`.
- Write-privilege probe: after connecting, classifies the session as writable, read-only, or unknown by inspecting `connectionStatus` (`showPrivileges: true`) for write-granting privileges/roles, with `hello` overriding the verdict to read-only when connected directly to a non-writable node (e.g. a secondary).
- **Multi-statement JS script execution (`SCRIPT_EXECUTION`)**: a buffer that does not parse as a single `db.` call or JSON query runs in a sandboxed QuickJS engine, executing every statement in source order — for example `db.users.insertOne({...}); db.orders.find({...});`. Each dispatched statement produces its own result set; `find()`/`aggregate()` return a real bounded JS `Array` (`.forEach`, `for...of`, `.map`, `.length`, `.toArray()` all work natively) capped at 10 000 documents, and `print()` output is captured into the primary result. Every dispatched operation is classified at the point it is actually constructed — not from source text — so a computed method name (`db.coll[name]()`) or an operation reached only inside a loop or conditional is still classified correctly; a script that could not be proven read-only requires one up-front confirmation, and any operation whose classification exceeds the confirmed ceiling aborts the script before it reaches the server. Each dispatched operation gets its own audit row, sharing one correlation id for the run. A mid-script driver error (e.g. a duplicate-key failure) stops the run at that statement without rolling back statements that already succeeded.

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

- Execution requests with a row limit (including zero) or statement timeout are rejected before dispatch because MongoDB cannot enforce those request protections. Unprotected requests retain their existing behavior.
- SQL is not supported; queries must use MongoDB shell-style syntax (or the JSON fallback).

- Instance metrics return a single data point per call (current snapshot from `serverStatus`), not a historical time series. Operations counters (e.g. `mongo.opcounters_insert`) grow monotonically — interpret them as deltas between samples rather than absolute rates.

- `$currentOp` requires the `inprog` privilege or `clusterMonitor` role on Atlas clusters. Without sufficient privileges, `fetch_inspector_snapshot("mongo.current_op")` returns an empty result set.
- Query cancellation is not supported (`QUERY_CANCELLATION` is not set).
- `RETURNING` is not supported; mutation capabilities also report no batch, no bulk update, and no bulk delete at the capability level (`supports_batch`, `supports_bulk_update`, `supports_bulk_delete` are all `false`), even though the generator can emit `updateMany`/`deleteMany` text.
- Parser coverage is intentionally scoped to the supported method set above, not the full interactive shell language; `distinct` is not surfaced as a query capability (`supports_distinct: false`).
- No joins, subqueries, unions, CTEs, window functions, or `EXPLAIN` at the query-capability level.
- Transactions are advertised at the capability level (`supports_transactions: true`) but without isolation levels, savepoints, nested transactions, read-only, or deferrable support.
- DDL is not transactional (`transactional_ddl: false`); create-database, create-collection, alter, views, and triggers are not supported.

- The script engine sandboxes by omission: no `require`, no module loader, and no filesystem/network/process-spawn globals are reachable from script code. Top-level `await`, `require(...)`, and `import` statements are rejected by name before anything dispatches.
- Sandbox resource limits: 64 MiB memory, 512 KiB stack, and a 30-second wall-clock deadline per script run. The deadline is JS-time only — an in-flight database call cannot be interrupted mid-flight; it is bounded separately by a server-side `maxTimeMS` and the connection's own cancel flag.
- A single `find()`/`aggregate()` call in a script is capped at 10 000 documents; exceeding the cap fails with an explicit error naming the limit rather than silently truncating the result. Lazy/streaming cursor semantics (`hasNext`, `next`, `limit`, `skip`, `sort`, `count` chained onto a `find()`/`aggregate()` result) are out of scope for v1 and throw naming the called method — dispatch the same limit/skip/sort as extra arguments to `find()` instead, or use `.toArray()` on the bounded result.
- Documents returned to a script are converted via relaxed extJSON, not canonical extJSON: plain JSON numbers stay numbers (so `doc.qty + 1` is arithmetic, not string concatenation), while types JSON has no representation for (ObjectId, Date, …) are wrapped (`{"$oid": ...}`, `{"$date": ...}`). This conversion does not round-trip exactly — a BSON `Double` of `1.0` becomes JSON `1` — which is acceptable for inspecting a read document but means a read result should not be echoed back verbatim as a write.
- A statement mid-script that fails against the server (e.g. a duplicate-key error) aborts the run at that point; statements already executed are not rolled back, and later statements never dispatch.
- The Aggregate view shows at most 1,000 result documents per run; a pipeline stage that fails on the server (an unknown operator, a `$merge` into a sharded target the user cannot write) is reported as a driver error, not validated ahead of time. Only the stage shape is checked before the run: a JSON array whose elements each name one `$` operator.
- Fields of embedded documents come back in key order, not in stored order: the value model keeps embedded documents in a sorted map. Top-level fields keep document order.
- A top-level field that holds `null` and one that is absent look the same in the grid (a browse fills absent top-level fields with `null`); nested fields are told apart and show as `missing`. The server-change check ignores top-level nulls for the same reason.
- Integers written from the grid are stored as `Int32` when they fit and as `Int64` otherwise, whatever width the field had before.
- The editor's shell parser does not read shell constructors such as `NumberDecimal(...)` or `ISODate(...)` inside JSON arguments; typed writes go through the grid's field edits.
- The server-change check compares a fresh read of the document with the page's copy just before writing; a change that lands between that read and the write is not detected.
- A text value of 24 hexadecimal digits in the query slots runs as an `ObjectId`, so such a string cannot be searched as text from the slots or the visual builder.
- Decimal values in builder conditions compare as doubles, so they do not match `Decimal128` fields.
- `{"$date": ...}` accepts only an RFC 3339 string; the numeric and `{"$numberLong": ...}` forms of extended JSON are not decoded.
