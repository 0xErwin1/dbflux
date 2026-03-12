# dbflux_driver_sqlite

## Features

- Embedded SQLite relational driver using file-based database paths.
- Supports SQL execution, schema discovery, views, indexes, foreign keys, check constraints, and unique constraints.
- Supports query cancellation via SQLite interrupt handles.
- Includes SQL/code generation for CRUD, indexes, reindex, create table, and drop table.

## Limitations

- Local file driver only; no network transport, SSH tunneling, or TLS/SSL mode.
- SQL-only driver; it does not expose document or key-value APIs.
- SQLite schema model has no server-side multi-schema namespace equivalent.
