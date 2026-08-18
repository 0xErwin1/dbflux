# ClickHouse Driver

## Features

- Blocking HTTP(S) transport using rustls and HTTP Basic authentication
- Arbitrary single-statement SQL with forced `JSONCompact` responses
- Database, table, view, column, engine, key, size, and compression introspection
- Lazy schema loading per ClickHouse database
- Recursive decoding for nullable, low-cardinality, array, tuple, map, decimal, integer, date/time, and JSON-like types
- Read-only visual SELECT generation and ClickHouse identifier/literal syntax
- Local chart authoring from query results

## Limitations

- No SSH tunnels
- No transactions, prepared statements, or query cancellation
- No structured INSERT, UPDATE, DELETE, DDL, or data transfer support; write SQL can only be entered explicitly
- One SQL statement per request
- HTTP response bodies are capped at 128 MiB
- Named ClickHouse time zones are not interpreted client-side; ISO timestamps with offsets are handled accurately, while offset-free timestamps are treated as UTC
