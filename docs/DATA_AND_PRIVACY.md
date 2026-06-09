# Data & Privacy

Where DBFlux stores your data, how it protects your credentials, what the audit
log keeps, and how to back up or fully reset.

## At a glance

| Your data | Where it lives |
|-----------|----------------|
| Connection profiles, settings, history, saved charts/queries, audit log | One SQLite file: `dbflux.db` in the data directory |
| Open tabs / session | The same `dbflux.db`, plus scratch files in the data directory |
| Passwords, passphrases, API secrets | Your **OS keyring** — never in `dbflux.db` |
| IPC/MCP auth token | A `0600` file in the config directory |

DBFlux keeps almost everything in a single SQLite database. Secrets are the
deliberate exception: they go to the operating system's keyring, and the database
only stores a *reference* to them.

---

## Data locations

DBFlux uses your platform's standard directories.

| Platform | Data directory | Config directory |
|----------|----------------|------------------|
| **Linux** | `~/.local/share/dbflux/` | `~/.config/dbflux/` |
| **macOS** | `~/Library/Application Support/dbflux/` | `~/Library/Application Support/dbflux/` |
| **Windows** | `%APPDATA%\dbflux\` | `%APPDATA%\dbflux\` |

The data directory holds:

- **`dbflux.db`** — the unified database (everything below in [What's in the
  database](#whats-in-the-database)).
- **`st_sessions/`** — scratch/shadow files for open editor tabs.

The config directory holds the **IPC auth token** (see [below](#ipcmcp-auth-token)).

### Stable vs. Nightly

A Nightly build uses a separate database file, `dbflux-nightly.db`, so a
pre-release migration can never touch your stable data. Stable and release
candidate builds both use `dbflux.db`.

You can make a Nightly build share the stable database via **Settings → General →
Storage → Use the stable database** (applies on next launch). Internally this
just drops an empty `use-stable-db` marker file in the data directory.

---

## What's in the database

`dbflux.db` is a single SQLite file. Its tables are grouped by prefix:

| Prefix | Contains |
|--------|----------|
| `cfg_*` | Configuration: connection profiles, auth/proxy/SSH tunnel profiles, RPC services, connection hooks, MCP governance, and the General/Audit settings. (Secret *values* are **not** here — only keyring references.) |
| `st_*` | Workbench state: open sessions/tabs, **query history** (full query text), saved queries, recent items, schema cache, UI state. |
| `aud_*` | The audit log and saved audit filters. |
| `viz_*` | Saved charts and dashboards. |
| `qry_*` | Visual Query Builder saved queries. |
| `sys_*` | Internal: schema migration version, app metadata. |

> **Note on query history.** The workbench query history (`st_*`) stores the
> **full text** of queries you run, in the clear. This is separate from the audit
> log, which fingerprints query text by default (see below). If you don't want
> query text retained, lower **Max history entries** in Settings → General, or
> clear history from the editor's history view.

---

## Secrets and the OS keyring

Passwords, SSH passphrases, proxy credentials, and provider secrets are stored in
your operating system's keyring, **not** in `dbflux.db`.

| Platform | Keyring backend |
|----------|-----------------|
| **Linux** | Secret Service (GNOME Keyring / KWallet, via libsecret) |
| **macOS** | Keychain |
| **Windows** | Windows Credential Manager |

All entries are stored under the service name **`dbflux`**. The database holds
only a reference string per secret:

| Secret | Reference |
|--------|-----------|
| Connection password | `dbflux:conn:<profile-id>` |
| Inline SSH password/passphrase | `dbflux:ssh:<profile-id>` |
| Saved SSH tunnel | `dbflux:ssh_tunnel:<tunnel-id>` |
| Proxy credential | `dbflux:proxy:<proxy-id>` |
| Auth-profile field | `dbflux:auth:<profile-id>:<field>` (one per field) |

### When secrets are (and aren't) saved

- A connection password is only stored when you tick **Save password**; SSH and
  proxy secrets only when you tick their **Save** checkbox.
- If no keyring is available, DBFlux hides the **Save** checkboxes and does not
  persist secrets — you re-enter them each session.
- A *locked* keyring still counts as available: writes may fail until you unlock
  it, but DBFlux keeps secret support enabled.

---

## Session and tabs restore

Which tabs you have open — their kind, file paths, order, active tab, and pin
state — is recorded in `dbflux.db` (`st_sessions` / `st_session_tabs`). The
actual scratch/shadow file contents live alongside it under `st_sessions/` in the
data directory. On startup DBFlux restores this session when **Settings → General
→ Restore session on startup** is on (the default).

---

## Audit and privacy

DBFlux logs significant operations (queries, connections, hooks, scripts, config
changes, MCP/governance decisions) to the audit log in `dbflux.db`. It's designed
to be privacy-preserving by default:

| Behavior | Default | Effect |
|----------|---------|--------|
| **Capture query text** | Off | Query text is replaced with a SHA-256 **fingerprint** plus its length — the full text is never stored in the audit row. |
| **Redact sensitive values** | On | Sensitive patterns (AWS keys, JWTs, connection strings with credentials, etc.) are replaced with `[REDACTED]`. |
| **Detail size cap** | 64 KiB | Oversized event payloads are truncated to a small partial envelope. |

Sensitive **JSON keys** (`password`, `token`, `secret`, `api_key`,
`access_key`, `session_token`, `connection_string`, `url`, …) are always redacted
— even if you turn off pattern-based redaction.

> Remember the [query-history caveat](#whats-in-the-database): the audit log
> fingerprints query text, but the *workbench history* stores it in full. They're
> two different stores.

For the complete event schema, categories, and the viewer, see
[Audit](AUDIT.md) and [Dashboards & Audit → Audit viewer](DASHBOARDS_AND_AUDIT.md#audit-viewer).

---

## IPC/MCP auth token

DBFlux exposes a local IPC surface (used by the MCP server and external RPC
services). It authenticates callers with a token stored at:

```
<config dir>/dbflux/ipc_auth_token
```

(on Linux, `~/.config/dbflux/ipc_auth_token`). It's a random value regenerated on
each startup, written with owner-only `0600` permissions, and also exported to the
`DBFLUX_IPC_TOKEN`, `DBFLUX_DRIVER_IPC_TOKEN`, and `DBFLUX_AUTH_PROVIDER_IPC_TOKEN`
environment variables for child processes.

This token is **process-identity only** — any local process that can read it can
connect. Do not expose the IPC/MCP surface beyond localhost without an additional
authentication layer. See [AI + MCP Integration](MCP_AI_INTEGRATION.md) for the
trust model.

---

## Backup and reset

DBFlux has no dedicated backup/restore command, but because everything lives in
one file, both are straightforward.

### Back up

Copy the single database file while DBFlux is closed:

```
~/.local/share/dbflux/dbflux.db        # Linux (adjust per platform)
```

That file contains your profiles, history, saved charts/queries, and audit log.
Your **secrets are not in it** — they stay in the OS keyring — so a copied
database on another machine will reference keyring entries that don't exist there
until you re-enter the secrets.

### Full reset

To wipe DBFlux's data:

1. Delete the **data directory** (`~/.local/share/dbflux/` on Linux) — removes the
   database and session files.
2. Delete the **config directory** (`~/.config/dbflux/` on Linux) — removes the
   IPC auth token.
3. **Clear keyring entries manually.** Secrets under the `dbflux` service remain
   in your OS keyring after deleting the directories; remove them with your
   platform's keyring tool if you want a complete wipe.

> Deleting the data directory is irreversible. Back up `dbflux.db` first if you
> might want your profiles or history back.

---

## Related

- [Settings & Hooks](SETTINGS.md) — the General/Audit/Storage controls referenced here.
- [Connecting → Advanced Setup](CONNECTIONS.md) — where secrets are entered.
- [Audit](AUDIT.md) — the full audit event schema and redaction details.
