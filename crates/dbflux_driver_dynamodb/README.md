# dbflux_driver_dynamodb

## Features

- DynamoDB document driver using `aws-sdk-dynamodb` with region/profile configuration and optional endpoint override.
- Table discovery with `ListTables`/`DescribeTable`, including PK/SK and GSI/LSI key metadata mapped into DBFlux schema abstractions.
- Native command envelope execution for `scan`, `query`, `put`, `update`, and `delete`.
- Read options for index targeting, consistent read control, and filter translation fallback policy.
- Mutation support for single-item and many-item paths, including bounded retry for unprocessed batch writes.
- Single-item upsert support with conditional update + put fallback.

## Limitations

- Query cancellation is not supported.
- Command envelope API does not expose PartiQL or DynamoDB transaction operations.
- `update` with `many=true` and `upsert=true` is not supported.
- Pagination in DBFlux collection browsing currently remains offset-based at the core request level.
