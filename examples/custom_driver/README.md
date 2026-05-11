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

RPC services are created from the DBFlux UI.

1. Build the example binary.
2. Start DBFlux normally.
3. Open `Settings → RPC Services`.
4. Add a new service with these values:
   - `Socket ID`: `my-test-driver.sock`
   - `Command`: the absolute path to `examples/custom_driver/target/debug/custom-driver`
   - `Args`: `--socket my-test-driver.sock`
   - Optional env: `RUST_LOG=info`
5. Save the service. DBFlux persists it in the internal SQLite-backed settings store.
6. Open the connection manager. You should see a new driver entry using metadata served by this custom driver (`Mock Database`).
7. Select it and fill the form fields:
   - `Endpoint` (required)
   - `API Key` (optional)
8. Save and connect.

## Process ownership

- If DBFlux starts this service from `Settings → RPC Services`, DBFlux tracks it and stops it on DBFlux shutdown.
- If you start the service manually and leave both `command` and `args` empty, DBFlux uses the running socket but does not own or stop that process.

## Notes

- The service key used internally by DBFlux is `rpc:<socket_id>`.
- If `command` and `args` are both omitted, DBFlux expects this service to already be running.
- If `command` is omitted but `args` is present, DBFlux launches `dbflux-driver-host`, and your `args` must include both `--driver` and `--socket` with the same socket ID.
- Use absolute paths for `command` while testing to avoid PATH issues.

## Queries to try

```sql
SELECT * FROM mockdb
INSERT INTO users VALUES (1, 'Test')
UPDATE users SET name = 'Updated' WHERE id = 1
DELETE FROM users WHERE id = 1
```

## Troubleshooting

- **Driver does not appear in UI**: check the Services settings panel and DBFlux logs for launch/probe diagnostics.
- **Connection refused**: ensure `socket_id` matches between config and `--socket` arg.
- **Permission denied**: ensure the binary in `command` is executable.
- **Version mismatch**: ensure example and DBFlux are built from compatible code.
- **No form fields appear**: verify your service returns `form_definition` in `Hello`.
- **Service disappeared after app restart**: re-open `Settings → RPC Services` and confirm the saved service still points to the same binary/socket.
