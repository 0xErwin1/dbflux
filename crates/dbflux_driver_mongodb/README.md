# dbflux_driver_mongodb

## Features

- MongoDB document driver with collection browsing, counting, and document CRUD flows.
- Supports MongoDB shell-style query parsing (`db.collection.method(...)`) and mutation query generation.
- Exposes document-focused schema metadata (fields and indexes).
- Supports SSH tunneling and URI/manual connection modes.
- Includes aggregation capability flags and document-tree-friendly value mapping.

## Limitations

- SQL syntax is not supported; queries must use MongoDB shell-style syntax.
- Query cancellation is not supported.
- Parser coverage is intentionally scoped to supported command patterns, not the full interactive shell language.
