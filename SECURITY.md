# Security Policy

## Reporting a vulnerability

Report privately through GitHub Security Advisories:

**https://github.com/0xErwin1/dbflux/security/advisories/new**

Please do not open a public issue for a suspected vulnerability. A report is more
useful with the DBFlux version, the platform, the driver involved when there is
one, and the smallest set of steps that reproduces it.

Machine-readable contact details are published at
[`/.well-known/security.txt`](https://dbflux.dev/.well-known/security.txt).

## Supported versions

Fixes land on the current release branch. DBFlux develops on `main` and cuts a
`release/vX.Y` branch per minor, which receives cherry-picked fixes until it
reaches end of life; older minors do not. See [the release
process](docs/RELEASE.md) for how the branches and channels work.

If you are running an older minor, the answer to a security report will be to
upgrade to the current one.

## Known limitations, by design

These are documented behaviours rather than vulnerabilities. A report about them
is welcome as a design discussion, but they are not treated as an undisclosed
risk.

- **MCP authentication is process identity only.** Presenting `--client-id` is
  the sole authentication signal, so any local process that knows the client id
  can connect. It is not a cryptographic guarantee, and the MCP server should not
  be exposed beyond localhost without an additional authentication layer. See
  [AI + MCP integration](docs/MCP_AI_INTEGRATION.md).
- **Connection hooks and Lua scripts run code you configured.** They execute with
  the privileges of the DBFlux process by design; that is what a hook is. See
  [Settings and hooks](docs/SETTINGS.md) and [Lua scripting](docs/LUA.md).
- **The audit log is local.** It records what happened on that machine and is
  readable by anything that can read your data directory. See [data and
  privacy](docs/DATA_AND_PRIVACY.md).

## Where secrets live

Credentials are held in the operating system keyring, never in a connection
profile file, and the audit log stores a fingerprint of query text rather than
the text itself. [Data and privacy](docs/DATA_AND_PRIVACY.md) describes what is
written where, and how to inspect or remove it.
