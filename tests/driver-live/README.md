# Driver live integration tests

Driver integration tests now use `testcontainers` for PostgreSQL, MySQL, MongoDB, and Redis.
Each test starts an isolated container with a dynamic host port and tears it down automatically.
SQLite integration uses a temporary local file.

## Run live driver tests

```bash
cargo test -p dbflux_driver_postgres --test live_integration -- --ignored
cargo test -p dbflux_driver_mysql --test live_integration -- --ignored
cargo test -p dbflux_driver_mongodb --test live_integration -- --ignored
cargo test -p dbflux_driver_redis --test live_integration -- --ignored
cargo test -p dbflux_driver_sqlite --test live_integration
```

The ignored tests require a working Docker daemon because `testcontainers` talks to Docker directly.
