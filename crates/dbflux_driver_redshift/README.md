# dbflux_driver_redshift

Amazon Redshift driver for DBFlux (read-only v1), built directly on the [`postgres`](https://crates.io/crates/postgres) wire client rather than on `dbflux_driver_postgres`.

## Features

- Relational driver (`DatabaseCategory::Relational`, `QueryLanguage::Sql`) that speaks the PostgreSQL wire protocol against a Redshift cluster or Redshift Serverless endpoint.
- Connection form with host, port (default `5439`), database, user, password, SSL/`sslmode` (`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full`), a connection-URI mode (`redshift://...`, normalized internally to `postgresql://...`), and SSH tunneling.
- Schema introspection over `information_schema` for databases, schemas, tables, views, and columns, with `ColumnKind` classification (timestamp/integer/float/text) mirroring standard PostgreSQL OIDs.
- Redshift-only extended types (`SUPER`, `VARBYTE`, `GEOMETRY`, `GEOGRAPHY`, `HLLSKETCH`) classify as `ColumnKind::Text` and render as text; any other unrecognized OID falls back to a defensive UTF-8 text decode instead of panicking.
- Table details surface Redshift-specific storage metadata through the generic `TableInfo.storage_hints` seam (read from `SVV_TABLE_INFO` and `PG_TABLE_DEF`): distribution key (`KEY`/`EVEN`/`ALL`/`AUTO`, with the key column when applicable) and sort key (compound or interleaved, with its ordered columns). Declared primary key, foreign key, and unique constraints are still surfaced through the standard core metadata shapes, each labeled advisory/non-enforced — Redshift accepts but never enforces these constraints, and no index list is fabricated from them.
- Query execution (`SELECT`/browse) returns rows with typed columns via the standard `Connection::execute` path; query cancellation is supported through the wire client's cancel token.
- `RedshiftErrorFormatter` maps common connection and query failures (timeouts, refused connections, authentication failures, unreachable clusters, `SQLSTATE`-bearing query errors) to clear, driver-formatted messages instead of raw debug output.

## Limitations

- Read-only: `DriverMetadata.capabilities` omits `INSERT`, `UPDATE`, `DELETE`, `RETURNING`, `BULK_INSERT`, `TRUNCATE_TABLE`, and all DDL/transactional-DDL flags. There is no inline grid edit and no mutation/visual-query builder for this driver. `Connection::execute` additionally rejects any non-read statement at the wire layer with an explicit error, so a write attempt never becomes a silent no-op.
- No `INDEXES` capability: Redshift has no true index structures, so `TableDetails.indexes` is always `None` rather than being synthesized from the (non-enforced) primary key.
- No triggers: Redshift does not support triggers, so none are discovered or surfaced.
- No IAM/SSO-based authentication. Only username/password (optionally via SSH tunnel) is supported; Redshift's IAM-based `GetClusterCredentials` and browser SSO flows are not implemented.
- No `COPY`/`UNLOAD` support and no data-transfer/bulk-export integration specific to Redshift.
- No query-plan visualization (`EXPLAIN` output is not parsed or rendered).
- No instance metrics or instance inspector (`INSTANCE_METRICS`/`INSTANCE_INSPECTOR` are not declared).
- The extended-type OID values used for `SUPER`/`VARBYTE`/`GEOMETRY`/`GEOGRAPHY`/`HLLSKETCH`, and the exact `SVV_TABLE_INFO`/`PG_TABLE_DEF` query shapes used for storage hints, are validated only by `#[ignore]`d live integration tests (`crates/dbflux_driver_redshift/tests/live_integration.rs`), since no local or Docker-based Redshift engine exists. Run them explicitly against a real cluster with `cargo nextest run -p dbflux_driver_redshift --run-ignored all`.
