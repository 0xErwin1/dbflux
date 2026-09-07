# Driver live integration tests

Driver integration tests use `testcontainers` for PostgreSQL, MySQL, MongoDB,
Redis, DynamoDB Local, SQL Server, and ClickHouse. Each test starts an isolated
container with a dynamic host port and tears it down automatically. SQLite
integration uses a temporary local file.

## Run live driver tests

```bash
cargo test -p dbflux_driver_postgres --test live_integration -- --ignored
cargo test -p dbflux_driver_mysql    --test live_integration -- --ignored
cargo test -p dbflux_driver_mongodb  --test live_integration -- --ignored
cargo test -p dbflux_driver_redis    --test live_integration -- --ignored
cargo test -p dbflux_driver_dynamodb --test live_integration -- --ignored
cargo test -p dbflux_driver_mssql    --test live_integration -- --ignored
cargo test -p dbflux_driver_clickhouse --test live_integration -- --ignored
cargo test -p dbflux_driver_sqlite   --test live_integration
```

The ignored tests require a working Docker daemon because `testcontainers`
talks to Docker directly.

## ClickHouse specifics

The ClickHouse suite pulls `clickhouse/clickhouse-server:25.8.30.16` (25.8
LTS). Every ignored test launches its own container through `testcontainers`,
maps HTTP port 8123 to a dynamic host port, and initializes database
`dbflux_test` with user/password `dbflux`/`dbflux`.

## SQL Server specifics

The MSSQL suite pulls `mcr.microsoft.com/mssql/server:2022-latest` (amd64
only — emulate or substitute Azure SQL Edge on arm64). Each test launches a
fresh container, creates a `dbflux_test` database, and runs against it.

Coverage includes:

- URI-mode connect (`use_uri = true`) for the bulk of the suite.
- Form-mode connect (`use_uri = false`, host/port/user + secret-fed password)
  in `mssql_form_mode_connect_query_and_select_db`.
- SSL modes `off` and `on` (with `trust_server_certificate = true`, since the
  stock image uses a self-signed cert) in `mssql_ssl_mode_off_connects` and
  `mssql_ssl_mode_on_trusts_self_signed`.
- CRUD via `OUTPUT INSERTED.*` / `OUTPUT DELETED.*`, schema introspection,
  `OFFSET ... FETCH NEXT` paging, cancellation via side-channel `KILL`.

`ssl_mode = "required"` and named-instance routing are not covered by the live
suite: the test image ships a self-signed cert (so strict-validation paths
would fail by design) and only the default `MSSQLSERVER` instance is exposed
(no SQL Browser sidecar). Both paths are covered by unit tests in
`crates/dbflux_driver_mssql/src/driver.rs::tests`.
