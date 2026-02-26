# Custom Driver Example

This example is a standalone driver host that speaks the DBFlux Driver RPC protocol.

It is intentionally small and in-memory, so you can verify the integration flow end-to-end before implementing a real database backend.

## What it implements

- Hello handshake (with `driver_kind` and `driver_metadata`)
- Driver-defined form (`endpoint`, `api_key`) served in `Hello`
- Session lifecycle (`OpenSession`, `CloseSession`, `Ping`)
- Basic query execution (`Execute`)
- `ListDatabases` and `Schema`
- Fallback `UnsupportedMethod` for unimplemented requests

## Build

From this directory:

```bash
cargo build
```

The binary will be generated at:

`examples/custom_driver/target/debug/custom-driver`

## Run manually (quick smoke test)

```bash
RUST_LOG=info cargo run -- --socket my-test-driver.sock
```

You should see:

`Custom driver listening on socket: my-test-driver.sock`

## Integrate with DBFlux

DBFlux now treats the external service as the source of truth for driver metadata. `config.json` only provides process/socket runtime configuration.

1. Build the example binary.
2. Create (or edit) `~/.config/dbflux/config.json`.
3. Add this service entry:

```json
{
  "rpc_services": [
    {
      "socket_id": "my-test-driver.sock",
      "command": "/ABSOLUTE/PATH/TO/examples/custom_driver/target/debug/custom-driver",
      "args": ["--socket", "my-test-driver.sock"],
      "env": {
        "RUST_LOG": "info"
      },
      "startup_timeout_ms": 5000
    }
  ]
}
```

4. Start DBFlux normally.
5. Open the connection manager. You should see a new driver entry using metadata served by this custom driver (`Mock Database`).
6. Select it and fill the form fields:
   - `Endpoint` (required)
   - `API Key` (optional)
7. Save and connect.

## Process ownership

- If DBFlux starts this service from `config.json`, DBFlux tracks it and stops it on DBFlux shutdown.
- If you start the service manually, DBFlux uses it but does not own/stop that process.

## Notes

- The service key used internally by DBFlux is `rpc:<socket_id>`.
- If `command` is omitted, DBFlux defaults to `dbflux-driver-host`.
- Use absolute paths for `command` while testing to avoid PATH issues.

## Queries to try

```sql
SELECT * FROM mockdb
INSERT INTO users VALUES (1, 'Test')
UPDATE users SET name = 'Updated' WHERE id = 1
DELETE FROM users WHERE id = 1
```

## Troubleshooting

- **Driver does not appear in UI**: check DBFlux logs for probe/launch errors.
- **Connection refused**: ensure `socket_id` matches between config and `--socket` arg.
- **Permission denied**: ensure the binary in `command` is executable.
- **Version mismatch**: ensure example and DBFlux are built from compatible code.
- **No form fields appear**: verify your service returns `form_definition` in `Hello`.
- **Service disappeared after app restart**: DBFlux reads config at startup; restart after changing `config.json`.
