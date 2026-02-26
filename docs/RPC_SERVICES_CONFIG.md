# RPC Services Config Reference

This file documents `~/.config/dbflux/config.json` for external driver services.

## Location

`~/.config/dbflux/config.json`

DBFlux reads this file at startup.

## Schema

```json
{
  "rpc_services": [
    {
      "socket_id": "my-driver.sock",
      "command": "/absolute/path/to/driver",
      "args": ["--socket", "my-driver.sock"],
      "env": {
        "RUST_LOG": "info"
      },
      "startup_timeout_ms": 5000
    }
  ]
}
```

## Fields

- `socket_id` (required): local socket name used by DBFlux and the service.
- `command` (optional): executable to run when DBFlux needs to start the service.
  - Default: `dbflux-driver-host`
- `args` (optional): process arguments.
- `env` (optional): environment variables for the spawned process.
- `startup_timeout_ms` (optional): max wait time for socket readiness after spawn.
  - Default: `5000`

## Semantics

- `socket_id` is used literally.
- DBFlux internally identifies each service as `rpc:<socket_id>`.
- Driver name/icon/category/form are **not** configured here.
  - They come from service `Hello` response (`driver_metadata`, `form_definition`).

## Minimal example (service provides everything)

```json
{
  "rpc_services": [
    {
      "socket_id": "my-test-driver.sock",
      "command": "/home/user/dbflux/examples/custom_driver/target/debug/custom-driver",
      "args": ["--socket", "my-test-driver.sock"]
    }
  ]
}
```

## Common mistakes

- Mismatched socket names between config and service args.
- Relative `command` path that does not resolve under DBFlux process environment.
- Editing config without restarting DBFlux.
- Service not implementing `Hello` fields required by current protocol.
