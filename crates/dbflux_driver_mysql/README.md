# dbflux_driver_mysql

## Features

- MySQL and MariaDB relational driver implementations in one crate.
- Supports SQL execution, schema discovery, indexes, foreign keys, check constraints, and unique constraints.
- Supports authentication, SSL, SSH tunneling, and URI/manual connection modes.
- Supports query cancellation through a dedicated cancel path (`KILL QUERY` flow).
- Includes SQL/code generation for CRUD, indexes, foreign keys, and table DDL operations.

## Limitations

- SQL-only driver; it does not expose document or key-value APIs.
- Cancellation depends on server permissions and connection state when `KILL QUERY` is issued.
- Code generation is scoped to supported MySQL/MariaDB constructs; unsupported generator IDs return `NotSupported`.
