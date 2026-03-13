# dbflux_driver_ipc

## Features

- IPC driver adapter that proxies DBFlux driver operations to out-of-process driver hosts over local sockets.
- Driver kind, metadata, and form definition come from runtime `Hello` handshake with the remote service.
- Supports optional managed host lifecycle (spawn, health wait, shutdown tracking) for configured RPC services.
- Persists and uses external-driver profile values through `DbConfig::External { kind, values }`.

## Limitations

- Requires a compatible driver host process and reachable socket.
- Effective feature set is constrained by the remote driver's advertised metadata and implementation.
- If launch config is not provided, DBFlux cannot auto-start unavailable driver hosts.
