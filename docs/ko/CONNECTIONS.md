# Connecting to a Database — Advanced Setup

This guide covers everything in the Connection Manager beyond the basic "host,
port, user, password" form: reaching a database through an SSH tunnel, a proxy,
or AWS SSM; authenticating with provider-driven Auth Profiles (AWS SSO); and
pulling individual field values from a secret manager or parameter store instead
of typing them in.

For the day-to-day flow (creating a connection, browsing the schema, running
queries) see the [Usage Guide](USAGE.md). This document picks up at the
Connection Manager's **Access** tab and the value-source selectors.

---

## The Access tab: how DBFlux reaches the database

Every connection uses exactly **one** access method, chosen from the **Access
Method** dropdown. Switching methods clears the settings of the others — a
connection is either Direct, or SSH, or Proxy, or SSM, never a combination.

| Method | What it does |
|--------|--------------|
| **Direct** | Connect straight to the host/port from the Main tab. Can still resolve per-field value sources (see [Value sources](#value-sources-secret-manager-parameter-store-auth-session)). |
| **SSH Tunnel** | Open a local port-forward through an SSH host, then connect through it. |
| **Proxy** | Route the connection through a SOCKS5 or HTTP/HTTPS proxy. |
| **SSM Port Forwarding** | Use AWS Systems Manager to port-forward to an instance, then connect through the tunnel. Requires the `aws` build feature. |

### What happens when you press Connect

DBFlux runs a fixed pre-connect pipeline before the driver ever opens a socket:

1. **Authenticating** — validate or refresh the selected Auth Profile's session
   (this is where an AWS SSO browser login can happen).
2. **Resolving values** — resolve every per-field value source (secret manager,
   parameter store, env var, auth-session field) and patch them into the config.
3. **Opening access** — establish the SSH tunnel, proxy, or SSM session (or
   nothing, for Direct).
4. **Driver connect + schema fetch** — the driver connects and DBFlux loads the
   shallow schema.

Connection **hooks** (if you have any bound) run at the PreConnect, PostConnect,
PreDisconnect, and PostDisconnect phases around this pipeline. See
[Settings & Hooks](SETTINGS.md#connection-hooks).

---

## SSH tunnels

You can use an SSH tunnel in two ways:

- **Reference a saved tunnel** — pick a tunnel profile you manage centrally in
  **Settings → SSH Tunnels**. Recommended when you reuse the same bastion across
  several connections.
- **Inline** — fill the SSH fields directly on the Access tab. You can later
  press **Save as tunnel** to promote it into a reusable profile.

### SSH fields

| Field | Notes |
|-------|-------|
| **Host** / **Port** | The SSH server. Port is typically `22`. |
| **Username** | SSH user. |
| **Auth method** | **Private Key** or **Password**. |
| **Key path** (Private Key) | Path to the private key. **Leave it empty to use your SSH agent or default keys** (`~/.ssh/id_rsa`, etc.). |
| **Key passphrase** (Private Key) | Optional; stored in your OS keyring when you tick **Save**. |
| **Password** (Password auth) | Stored in your OS keyring when you tick **Save**. |

There is no separate "SSH agent" option — agent-based auth is what you get when
you choose **Private Key** and leave the key path blank.

**Test SSH** verifies the tunnel without saving the connection.

### Where SSH secrets live

Passphrases and passwords are stored in the **OS keyring**, never in the
database. The **Save** checkbox only appears when a keyring is available; if it
isn't, secrets aren't persisted and you'll re-enter them each session. See
[Data & Privacy → Secrets](DATA_AND_PRIVACY.md#secrets-and-the-os-keyring).

---

## Proxies

Proxies are managed in **Settings → Proxies**; the Access tab only *selects* a
saved proxy and shows its details. If you have none, the tab links you to
Settings.

| Field | Notes |
|-------|-------|
| **Type** | `SOCKS5`, `HTTP`, or `HTTPS`. Default port: `1080` for SOCKS5, `8080` for HTTP/HTTPS. |
| **Host** / **Port** | The proxy endpoint. |
| **Auth** | `None`, or `Basic` with a username (the password is stored in the keyring). |
| **No Proxy** | Comma-separated hosts/patterns to bypass. Supports `*` (all), exact hosts, and suffix matches (with or without a leading dot), case-insensitive. **CIDR ranges are not supported.** |
| **Enabled** | When a proxy profile is disabled, the connection falls back to a **direct** connection (with a warning) instead of failing. |

> **Heads-up:** a disabled proxy, or a remote host that matches **No Proxy**,
> results in a silent direct connection. If you expected traffic to go through
> the proxy and it didn't, check both of these first.

---

## Auth Profiles (AWS SSO and shared credentials)

Auth Profiles hold provider-driven authentication that DBFlux resolves at
connect time. They're created in **Settings → Auth Profiles** and selected per
connection. In this build the built-in providers are **AWS only**:

| Provider | Use it for |
|----------|------------|
| **AWS SSO** | IAM Identity Center (SSO) login that resolves an account + role. |
| **AWS SSO Session** | A reusable SSO session (Start URL + region + scopes) that AWS SSO profiles can inherit from. |
| **AWS Shared Credentials** | A named profile written to `~/.aws/credentials` (access key / secret / optional session token). |

Externally registered RPC auth providers can add more entries here; see
[RPC Services](RPC_SERVICES_CONFIG.md).

> AWS profiles that DBFlux reflects live from your `~/.aws/config` appear as
> **read-only** — you can select them but not edit them here; edit the AWS files
> directly.

### Creating an AWS SSO profile

The form is provider-driven. For AWS SSO you fill:

| Field | Notes |
|-------|-------|
| **Profile name** | e.g. `dev`. |
| **SSO session** | Optional reference to an **AWS SSO Session** profile. When set, Start URL and Region are inherited and their inline fields grey out. |
| **SSO Start URL** | Your Identity Center portal URL (skip if using a session). |
| **Region** | e.g. `us-east-1`. |
| **Account** | A dropdown that populates **after you log in** — it lists the accounts your SSO session can access. |
| **Role** | A dropdown that populates once an account is chosen. |

The **Account** and **Role** dropdowns are dynamic: they require a live SSO
session and refresh as their dependencies change. If they're empty, log in first
(see below).

The **SSO Wizard** offers the same flow as a guided, step-by-step creator:
enter name, Start URL, and region; it logs you in and then lists accounts and
roles for you to pick.

### The SSO login flow

When a connection (or the Account/Role dropdowns) needs an SSO session, DBFlux
opens a login modal:

- It **opens your browser automatically** to the verification URL.
- If the browser can't be opened, the modal shows the URL with a **Copy URL**
  action so you can open it manually.
- DBFlux continues automatically once you finish authenticating in the browser.
- **SSO login times out after 5 minutes.**

### Selecting an Auth Profile per connection

- **Direct mode** — the Auth Profile is *optional*. Use it only to resolve
  Secret/Parameter/Auth value sources (next section).
- **SSM mode** — the Auth Profile is **required**.
- If any field uses a Secret/Parameter value source whose provider is an auth
  provider, a matching Auth Profile is **required** or the connection is rejected
  before connecting.

Each profile row has **Manage**, **Login**, and **Refresh** buttons. **Login**
is enabled only when the selected profile actually needs a login.

---

## SSM Port Forwarding (managed access)

"Managed" access lets a provider open the path to the host for you. The shipped
implementation is **AWS SSM Port Forwarding** (`aws` feature required).

| Field | Notes |
|-------|-------|
| **Instance ID** | Target EC2 instance. Supports a value-source selector. |
| **Region** | Defaults to `us-east-1` if left blank. |
| **Remote Port** | The port on the instance to forward to. |
| **Auth Profile** | **Required** — the AWS profile used to start the SSM session. |

The **local** tunnel port is assigned automatically by DBFlux and the OS — only
the remote port is configurable.

---

## Value sources: Secret Manager, Parameter Store, Auth Session

Any individual connection field (host, password, etc.) can pull its value from
an external source instead of a literal. Click the source selector next to a
field and choose:

| Source | What it does |
|--------|--------------|
| **Literal** | The value you type (default). |
| **Environment Variable** | Read from an env var by name. |
| **Secret Manager** | Fetch from a secret provider (AWS Secrets Manager). |
| **Parameter Store** | Fetch from a parameter provider (AWS SSM Parameter Store). |
| **Auth Session Field** | Pull a field from the resolved Auth Profile's session/credentials. |

Notes:

- Secret/Parameter sources can target a **JSON key** inside a JSON secret, so one
  stored JSON document can feed several fields.
- Resolved values are cached for **5 minutes** to avoid re-fetching on every
  reconnect.
- Secret/Parameter sources backed by an auth provider **require an Auth Profile**
  on the connection (enforced before connecting).

---

## Form mode vs. direct URI

Most relational drivers let you supply connection details either as individual
fields or as a single connection string. A **Use URI** toggle on the Main tab
switches between them.

- URI mode is available for **PostgreSQL, MySQL/MariaDB, SQL Server, MongoDB, and
  Redis**. SQLite, DynamoDB, CloudWatch, and InfluxDB use their own field-based
  forms instead.
- With URI mode **on**, the single URI field is authoritative and the individual
  fields are ignored; with it **off**, the fields are used.
- A password embedded in a URI is extracted and stored separately (in the
  keyring when you save it), not kept in the URI text.
- When you connect through an SSH/proxy/SSM tunnel, DBFlux always uses the
  field-based config (rewritten to `127.0.0.1:<local port>`), even if you typed a
  URI.

---

## Quick reference: gotchas

- **One access method per connection.** Switching methods clears the others.
- **Secrets live in your OS keyring**, never in the DBFlux database. If the
  keyring is unavailable, "Save" checkboxes disappear and secrets aren't kept.
- **A disabled proxy or a No-Proxy match silently connects directly.**
- **`No Proxy` does not support CIDR** — list hosts/suffixes, not IP ranges.
- **SSM and auth-backed value sources both require an Auth Profile.**
- **Only AWS auth providers are built in** (SSO, SSO Session, Shared
  Credentials). Other providers come from external RPC services.
- **SSO login auto-opens the browser and times out after 5 minutes.**

## Related

- [Usage Guide](USAGE.md) — the basic connect/query/results flow.
- [Settings & Hooks](SETTINGS.md) — managing SSH/proxy/auth profiles and hooks.
- [Data & Privacy](DATA_AND_PRIVACY.md) — where credentials and data are stored.
- [RPC Services](RPC_SERVICES_CONFIG.md) — external drivers and auth providers.
