# Telemetry TLS test fixtures

Static, public test-only certificates for the `tls_live_*` unit tests in `crates/dbflux_telemetry`. They let the suite exercise the real production HTTPS transport against an in-process rustls server using loopback only, without external network access or changes to trust stores or configuration.

| File | Role |
|---|---|
| `ca.der` | Self-signed test root CA (DER), added by tests only via `add_root_certificate` |
| `server.der` | Leaf certificate (DER) presented by the in-process test server |
| `server-key.pk8.der` | Leaf private key (unencrypted PKCS#8 DER) for the test server |

## Fixed properties

- **Validity**: 2020-01-01 through 2040-01-01 (UTC). Tests require the system clock to be within this interval; regenerate the fixtures before they expire.
- **Names**: the leaf carries exactly one SAN, DNS `localhost`. There is no IP SAN, so connecting to `https://127.0.0.1/...` with this CA must fail hostname validation — that failure is an intentional test vector.
- **Constraints**: CA is `critical, CA:TRUE` with `keyCertSign`/`cRLSign` only; leaf is `CA:FALSE`, `digitalSignature`/`keyEncipherment`, `serverAuth`.

## Not production material

These keys and certificates are committed public fixture data. They are not secrets, must never be installed in a trust store, and must never be used for anything other than loopback telemetry tests. Regenerate them locally with `openssl` if new fixtures are ever needed.
