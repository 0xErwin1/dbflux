# dbflux_driver_dynamodb

AWS DynamoDB driver for DBFlux, built on the [`aws-sdk-dynamodb`](https://crates.io/crates/aws-sdk-dynamodb) SDK.

## Features

- Managed NoSQL driver classified as `DatabaseCategory::Document` with a `QueryLanguage::Custom("DynamoDB")` command envelope; the editor uses a DynamoDB-specific syntax, not SQL.
- AWS connection configuration via region, named profile, and optional endpoint override (for DynamoDB Local or VPC endpoints). `deployment_class` is `CloudManaged`.
- Table discovery with `ListTables` and `DescribeTable`, mapping partition key (PK), sort key (SK), and Global/Local Secondary Index (GSI/LSI) key metadata into DBFlux schema abstractions.
- Native command envelope execution for `scan`, `query`, `put`, `update`, and `delete`. The query generator emits scan-shaped preview envelopes and notes that execution may optimize to `Query` when the filter matches the table key schema.
- Read options for index targeting, consistent-read control, and a filter-translation fallback policy (server-side filter vs. client-side fallback; client filtering is rejected when the fallback policy is set to reject).
- WHERE operators in semantic filters: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `NotIn`, and logical `And`/`Or` (see Limitations for `Not`).
- Mutations: insert (`put`), update, and delete (`INSERT`/`UPDATE`/`DELETE`). Batch writes support up to 25 items (`max_insert_values: 25`, `supports_batch: true`) with bounded retry for unprocessed batch-write items.
- Single-item upsert support via a conditional update with a put fallback; the key map is resolved from either the filter or the update payload (partition key required, sort key required when the table defines one).
- Many-item update path (`update` with `many=true`) using a shared update expression.
- Nested documents and arrays mapped into the document-tree view (`NESTED_DOCUMENTS`, `ARRAYS`).
- DDL: drop table (`supports_drop_table: true`).
- Pagination via page tokens (`PaginationStyle::PageToken`).

## Limitations

- The `profile` field (AWS named profile) is marked `RequiredOnImport` on bundle export. Recipients must supply or create a matching auth profile at import time; the field value is omitted from the exported bundle rather than embedded.
- Query cancellation is not supported; the driver returns `NotSupported` for cancel requests.
- The command envelope API does not expose PartiQL or DynamoDB transaction operations; transactions are disabled (`supports_transactions: false`).
- Single-item upsert is supported (`supports_upsert: true`); `update` with `many=true` and `upsert=true` together is rejected (`update_many_with_upsert`).
- Bulk update and bulk delete are not supported (`supports_bulk_update: false`, `supports_bulk_delete: false`), and `RETURNING` is not supported.
- Semantic filters do not support `NOT` expressions or operators outside the supported set; unsupported operators return `NotSupported`.
- No SSL form (TLS is handled by the AWS SDK transport), no schemas, and no DDL beyond drop-table (no create/alter table, no index creation).
- Aggregate requests are not supported by the semantic planner.
- Collection browsing in the core request layer remains offset-based, while the underlying API is page-token based.
