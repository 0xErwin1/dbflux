---
name: dbflux
description: Operate DBFlux, a keyboard-first database client, and its local MCP server for AI-driven database access.
---

# DBFlux

DBFlux is a keyboard-first database client built with Rust and GPUI. It
supports SQLite, PostgreSQL, MySQL/MariaDB, SQL Server, MongoDB, Redis,
DynamoDB, CloudWatch, InfluxDB, Redshift, and S3.

Install: https://docs.dbflux.dev/install/

## Agent integration

DBFlux ships a local MCP server started with:

```
dbflux mcp --client-id <id>
```

It speaks JSON-RPC over stdio and is governed by policy, approval, and audit
layers:

- Operations are classified by impact (metadata, read, write, destructive,
  admin-safe, admin, admin-destructive).
- Destructive or write operations can require human approval before they run.
- Every governed operation is recorded in an audit log.

Authentication is process-identity only: presenting `--client-id` is the sole
authentication signal, so do not expose the MCP server beyond localhost
without an additional authentication layer.

## Links

- Documentation: https://docs.dbflux.dev/
- Source: https://github.com/0xErwin1/dbflux
