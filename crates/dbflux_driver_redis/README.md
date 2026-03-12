# dbflux_driver_redis

## Features

- Redis key-value driver covering string, hash, list, set, sorted set, and stream operations.
- Supports key scanning, key type discovery, TTL operations, key rename, and bulk key get.
- Supports multiple logical databases (`SELECT`) and Redis command-language execution.
- Supports authentication, SSL/TLS, SSH tunneling, and URI/manual connection modes.
- Includes command generation for key-value mutations.

## Limitations

- SQL syntax is not supported; use Redis command syntax.
- Query cancellation is not supported.
- SSH tunneling is not supported when URI mode is enabled.
