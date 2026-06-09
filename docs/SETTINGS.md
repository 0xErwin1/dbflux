# Settings & Connection Hooks

A reference for every Settings section and for connection hooks — the commands,
scripts, or Lua snippets DBFlux runs around a connection's lifecycle.

Open Settings from the command palette (**Open Settings**) or the sidebar. The
window is organized into sections down the left side.

| Section | Covers |
|---------|--------|
| [General](#general) | App-wide behavior: theme, startup, refresh, query safety. |
| [Audit](#audit) | What the audit log captures and how long it's kept. |
| [Keybindings](#keybindings) | Browse the keymap (read-only). |
| [Auth Profiles](#auth-profiles-proxies-ssh-tunnels) | AWS SSO / shared-credentials profiles. |
| [Proxies](#auth-profiles-proxies-ssh-tunnels) | SOCKS5 / HTTP proxy profiles. |
| [SSH Tunnels](#auth-profiles-proxies-ssh-tunnels) | Reusable SSH tunnel profiles. |
| [Services](#services-rpc) | External RPC drivers and auth providers. |
| [Hooks](#connection-hooks) | Reusable connection-hook definitions. |
| [Drivers](#drivers) | Per-driver overrides and settings. |

MCP-related sections (Clients, Roles, Policies) appear only when the binary is
built with the `mcp` feature; see [AI + MCP Integration](MCP_AI_INTEGRATION.md).

---

## General

### Appearance

| Setting | Options | Default |
|---------|---------|---------|
| **Theme** | Dark, Mirage, Light | Dark |
| **Style** | Default, Compact | Default |

### Startup & session

| Setting | Default | What it does |
|---------|---------|--------------|
| **Restore session on startup** | On | Reopen the tabs you had open last time. |
| **Reopen last connections** | Off | Reconnect to the connections that were active. |
| **Default focus** | Sidebar | Where focus lands on launch (Sidebar or the last tab). |
| **Max history entries** | 1000 | Query-history cap (minimum 10). |
| **Auto-save interval (ms)** | 2000 | How often editor buffers auto-save (minimum 500). |

### Refresh & background

| Setting | Default | What it does |
|---------|---------|--------------|
| **Default refresh policy** | Manual | Manual or Interval auto-refresh for data views. |
| **Default refresh interval (seconds)** | 5 | Interval used when the policy is Interval (minimum 1). |
| **Max concurrent background tasks** | 8 | Cap on simultaneous background work (minimum 1). |
| **Pause auto-refresh on error** | On | Stop auto-refreshing a view after it errors. |
| **Auto-refresh only if tab is visible** | Off | Skip refreshing tabs you're not looking at. |

### Execution safety (dangerous-query confirmation)

These three settings govern how DBFlux treats risky queries across **all**
drivers and query languages. There is no per-database toggle — the same rules
apply to SQL `DELETE`/`DROP`/`TRUNCATE`, MongoDB `deleteMany`/`drop`, Redis
`FLUSHALL`/`FLUSHDB`, and so on.

| Setting | Default | What it does |
|---------|---------|--------------|
| **Confirm dangerous queries** | On | Show a confirmation before running a dangerous query. Turn off to allow them without prompting. |
| **Require WHERE for DELETE/UPDATE** | On | Treat a `DELETE`/`UPDATE` with no `WHERE` as dangerous. |
| **Always require preview (ignore suppressions)** | Off | Force the confirm/preview modal even for queries you previously chose to stop confirming. |

### Storage (Nightly builds only)

| Setting | Default | What it does |
|---------|---------|--------------|
| **Use the stable database** | Off | Make a Nightly build share the stable `dbflux.db` instead of `dbflux-nightly.db`. Applies on next launch. |

See [Data & Privacy](DATA_AND_PRIVACY.md#data-locations) for how the Nightly and
stable databases are separated.

---

## Audit

The Audit section controls the unified audit log. The main user-facing control
is **Log Capture → Minimum Level** (trace / debug / info / warn / error), which
sets how much of DBFlux's internal logging is folded into the audit trail. Saving
takes effect without a restart.

Retention (how long events are kept) drives a periodic background purge when
configured. For the day-to-day audit experience — opening the viewer, filtering,
exporting — see [Dashboards & Audit](DASHBOARDS_AND_AUDIT.md#audit-viewer). For
the full event schema and redaction behavior see [Audit](AUDIT.md) and
[Data & Privacy](DATA_AND_PRIVACY.md#audit-and-privacy).

---

## Keybindings

This section is a **read-only viewer**. It lists the active keymap grouped by
context, with a text filter and inline warnings when a chord is bound to more
than one command. It does **not** currently let you rebind or save custom
shortcuts from the UI. Use it to discover and verify bindings; the full default
keymap is documented in [Usage → Keyboard Reference](USAGE.md#7-keyboard-reference).

---

## Auth Profiles, Proxies, SSH Tunnels

These three sections manage the reusable profiles you then select per connection
on the Access tab. They're documented in full — fields, AWS SSO flow, no-proxy
rules, SSH auth methods — in
[Connecting to a Database → Advanced Setup](CONNECTIONS.md):

- [Auth Profiles](CONNECTIONS.md#auth-profiles-aws-sso-and-shared-credentials)
- [Proxies](CONNECTIONS.md#proxies)
- [SSH Tunnels](CONNECTIONS.md#ssh-tunnels)

Credentials entered here are stored in your OS keyring, not the database. See
[Data & Privacy → Secrets](DATA_AND_PRIVACY.md#secrets-and-the-os-keyring).

---

## Services (RPC)

External drivers and auth providers run as separate processes that DBFlux talks
to over a local socket. Each service you add here has:

| Field | Notes |
|-------|-------|
| **Socket ID** | Unique identifier, used as the socket filename. ASCII letters, digits, `.`, `_`, `-` only. |
| **Command** | The executable to launch (optional for some setups). |
| **Startup Timeout (ms)** | How long to wait for the process to come up. Default 5000. |
| **Service Type** | **Driver** or **Auth Provider**. |
| **Enable this service** | Whether the service starts. Default on. |
| **Arguments** | Ordered process arguments. |
| **Environment Variables** | `KEY=value` pairs passed to the process. |

Changes here **take effect on the next launch**. Full reference:
[RPC Services Config](RPC_SERVICES_CONFIG.md) and the
[Driver RPC Protocol](DRIVER_RPC_PROTOCOL.md).

---

## Drivers

Pick a driver to see and override its behavior. Two groups are editable:

**Global overrides** — per-driver versions of the General settings. Each is a
tri-state (Inherit / On / Off, or an explicit value); leaving it on *Inherit*
uses the General default shown next to the control:

- Refresh policy and interval
- Confirm dangerous queries
- Require WHERE
- Require preview

**Driver settings** — options defined by the driver itself (rendered generically
from the driver's own schema, so the available fields depend on the driver).

The section also shows, read-only, the driver's **capability matrix**, category,
and query language.

---

## Connection Hooks

Hooks are reusable commands, scripts, or Lua snippets that run around a
connection's lifecycle. You **define** them globally in **Settings → Hooks**, then
**bind** them to phases on individual connections in the Connection Manager's
**Hooks** tab.

### Quick path

1. **Settings → Hooks → add a hook.** Give it a **Hook ID**, pick a **Type**, and
   fill in the command/script.
2. Open a connection in the **Connection Manager → Hooks tab**.
3. Select your hook in one of the four phase dropdowns (Pre-connect, Post-connect,
   Pre-disconnect, Post-disconnect).
4. Connect. Hook output streams into the **Tasks** panel.

### Hook types

| Type | What it runs | What you provide |
|------|--------------|------------------|
| **Command** | An executable | A command and space-separated arguments. |
| **Script** | A Bash or Python file | A language, a file path, and an optional interpreter override (blank = `bash` / `python3`, platform-adjusted). |
| **Lua** | An in-process Lua script | A file path and a set of capabilities (see below). Lua runs inside DBFlux — no external interpreter. |

Scripts are edited in DBFlux's editor and stored under a `hooks/` folder by
default.

#### Lua capabilities

A Lua hook only gets the abilities you enable:

| Capability | Default | Grants |
|------------|---------|--------|
| **Logging** | On | Write to the hook's output. |
| **Environment read** | On | Read environment variables. |
| **Connection metadata** | On | Read the connecting profile's metadata. |
| **Controlled process run** | Off | Call `dbflux.process.run(...)` to launch external processes. |

> Enabling **Controlled process run** lets the hook execute arbitrary external
> commands. DBFlux shows a security warning when it's on, both in the hook
> definition and on the per-connection binding. Enable it only for hooks you
> trust.

The embedded Lua runtime (available APIs, sandboxing) is documented in
[Lua Scripting](LUA.md).

### Hook options

| Option | Notes |
|--------|-------|
| **Enabled** | Disabled hooks are skipped. |
| **Working Directory** | Process/script cwd (not used by Lua). |
| **Environment** | Extra `KEY=value` pairs. |
| **Inherit parent environment** | On by default; pass DBFlux's env to the hook. |
| **Env Denylist** | Variable names to strip from the inherited env. |
| **Timeout (ms)** | Blank = no timeout. On timeout the process group is killed. |
| **Execution mode** | **Blocking** (default) waits for the hook; **Detached** runs in the background and does not block connect/disconnect. |
| **Ready signal** (Detached) | Text DBFlux waits for in the hook's output before continuing. |
| **On Failure** | The failure policy — see below. |

DBFlux always injects context env vars into process hooks: `DBFLUX_PROFILE_ID`,
`DBFLUX_PROFILE_NAME`, `DBFLUX_DB_KIND`, and, when known, `DBFLUX_HOST`,
`DBFLUX_PORT`, `DBFLUX_DATABASE`.

> **Secrets never leak into hooks by accident.** On top of your Env Denylist,
> DBFlux always strips inherited variables whose name contains `SECRET`, `TOKEN`,
> `PASSWORD`, or `_KEY`, and any `AWS_*` variable.

### Failure policies

What happens when a hook fails (non-zero exit, timeout, or error):

| Policy | Effect |
|--------|--------|
| **Disconnect** (default) | Abort the phase — the connect or disconnect flow stops. |
| **Warn** | Continue, but surface a warning. |
| **Ignore** | Continue; the failure is only logged. |

### Phases

| Phase | Runs |
|-------|------|
| **Pre-connect** | Before the connection opens. |
| **Post-connect** | After a successful connect. |
| **Pre-disconnect** | Before disconnecting. |
| **Post-disconnect** | After disconnecting. |

A connection's Hooks tab has one dropdown per phase (plus an "Extra" input for
binding additional hook IDs). The dropdowns list the reusable hooks you defined
in Settings → Hooks. Each hook runs as its own background task with live
stdout/stderr in the Tasks panel; output is capped at 4 MiB per hook.

---

## Related

- [Usage Guide](USAGE.md) — core workflow and keyboard reference.
- [Connecting → Advanced Setup](CONNECTIONS.md) — SSH, proxy, auth, value sources.
- [Data & Privacy](DATA_AND_PRIVACY.md) — where settings and secrets are stored.
- [Lua Scripting](LUA.md) — the embedded Lua runtime for hooks.
