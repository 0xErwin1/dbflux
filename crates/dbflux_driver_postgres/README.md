# dbflux_driver_postgres

## Features

- PostgreSQL relational driver with SQL query execution and schema discovery.
- Supports schemas, tables, views, indexes, foreign keys, check constraints, unique constraints, and custom types.
- Supports authentication, SSL, SSH tunneling, and URI/manual connection modes.
- Supports query cancellation through PostgreSQL cancel tokens.
- Includes PostgreSQL-specific SQL/code generation for CRUD, indexes, reindex, foreign keys, and type operations.

## Limitations

- SQL-only driver; it does not expose document or key-value APIs.
- Cancellation is best effort and depends on server/session state at cancellation time.
- Code generation targets supported PostgreSQL constructs only; unsupported generator IDs return `NotSupported`.
