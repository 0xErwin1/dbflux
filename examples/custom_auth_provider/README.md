# Custom Auth Provider Example

This example is a standalone auth-provider host that speaks the DBFlux auth-provider RPC protocol.

It is intentionally small and deterministic so you can validate the end-to-end integration before implementing a real OAuth, SSO, or cloud-credentials provider.

## What it implements

- Hello handshake with provider identity and auth form definition
- Session validation via `ValidateSession`
- Login flow via `Login`, including an optional verification URL progress event
- Runtime credential resolution via `ResolveCredentials`
- Fallback `UnsupportedMethod` for anything outside the minimal auth-provider contract

## Build

From this directory:

```bash
cargo build
```

The binary will be generated at:

`examples/custom_auth_provider/target/debug/custom-auth-provider`

## Run manually (quick smoke test)

```bash
RUST_LOG=info cargo run -- --socket my-test-auth.sock
```

You should see:

`Custom auth provider listening on socket: my-test-auth.sock`

## Integrate with DBFlux

RPC services are created from the DBFlux UI.

1. Build the example binary.
2. Start DBFlux normally.
3. Open `Settings → RPC Services`.
4. Add a new service with these values:
   - `Service Kind`: `Auth Provider`
   - `Socket ID`: `my-test-auth.sock`
   - `Command`: the absolute path to `examples/custom_auth_provider/target/debug/custom-auth-provider`
   - `Args`: `--socket my-test-auth.sock`
   - Optional env: `RUST_LOG=info`
5. Save the service.
6. Restart DBFlux so the new RPC service is probed on startup.
7. Open `Settings → Auth Profiles`.
8. Create a new auth profile using the provider exposed by this example (`Example Device Auth`).
9. Fill the fields:
   - `Region` (required)
   - `Access Key ID` (required)
   - `Session State` (`login_required`, `valid`, or `expired`)
   - `Verification URL` (optional)
10. Save the auth profile and run a login/credential resolution flow through DBFlux.

## Process ownership

- If DBFlux starts this service from `Settings → RPC Services`, DBFlux tracks it and stops it on shutdown.
- If you start the service manually and leave both `command` and `args` empty in the UI, DBFlux uses the running socket but does not own or stop that process.

## Notes

- Auth-provider services never appear as database drivers.
- For managed auth-provider services, DBFlux requires an explicit `command`; it does not assume `dbflux-driver-host`.
- DBFlux injects its auth-provider handshake token automatically when it launches the service.
- Use absolute paths for `command` while testing to avoid PATH issues.

## Troubleshooting

- **Provider does not appear in Auth Profiles**: confirm the service is enabled in `Settings → RPC Services`, then restart DBFlux and check the logs for probe failures.
- **Connection refused**: ensure `socket_id` matches between the saved service and the `--socket` argument.
- **Version mismatch**: ensure the example and DBFlux were built from compatible code.
- **Login never shows a browser URL**: set `Verification URL` in the auth profile fields; otherwise the example returns no progress URL.
- **Credential resolution fails**: ensure `Access Key ID` and `Region` are populated in the saved auth profile.
