# Privacy Policy

DBFlux is a local-first desktop application. Optional telemetry has three
levels: Off sends nothing, Minimal sends a build ping and one weekly activity
event, and Basic (preselected in the Welcome dialog) sends a weekly report with
aggregate usage counts instead of the activity event. Nothing is sent until you
confirm the Welcome disclosure. It never sends person identifiers, queries,
results or connection metadata. The project website sets no cookies and loads no third-party
scripts. This document describes those boundaries and the infrastructure
providers that see traffic on the way to you.

## Quick path

1. Database traffic goes directly to the servers you configure, with only what
   is needed to run the queries you ask for; optional telemetry is separate.
2. Telemetry has three levels: Off, Minimal and Basic. Basic is preselected in
   the Welcome dialog, but nothing is sent until you confirm the disclosure,
   and Off sends nothing. It sends no queries, results, credentials, file
   paths, or connection metadata — ever. Details are in [Telemetry](#telemetry-optional).
3. There is no crash reporting, no usage analytics beyond the telemetry above,
   and no account.
4. The website and documentation are static pages. They set no cookies, load
   no third-party script, and keep no record of individual visitors. An
   anonymous measurement of page usage, handled by the project's own server,
   is described under [Website usage measurement](#website-usage-measurement);
   it is not active yet.

## The application

| Question | Answer |
|----------|--------|
| Does DBFlux send usage data anywhere? | Only after you have confirmed the Welcome disclosure. Minimal sends a build ping and a weekly activity event, with no usage data. Basic, preselected in the Welcome dialog, replaces the activity event with a weekly report that includes usage counts. Off sends nothing. See [Telemetry](#telemetry-optional). |
| Does it check for updates? | Yes, unless you turn it off. At startup it asks the GitHub API (`api.github.com`) for the latest releases; no identifier is sent. Disable it with "Check GitHub for a new release at startup" in Settings › Updates & privacy. |
| What network connections does it open? | The ones you configure: database servers, SSH tunnels, proxies, cloud APIs, and the telemetry endpoint under the conditions below. |
| Where does my data live? | On your machine, in one SQLite file plus the operating system keyring for secrets. |
| Does it keep a record of what I do? | Local history and the audit log record work on your machine. Basic also keeps bounded aggregate counters; only telemetry reports can be delivered automatically under the conditions below. |
| Is anything shared with the project? | Telemetry events reach the project's collector at `https://telemetry.dbflux.dev` once you have confirmed the Welcome disclosure and unless the level is Off. Bug reports and logs reach the project only when you attach them to an issue yourself. |

Connections to a database, an SSH host, a proxy, or a cloud provider go
directly from your machine to the server you named. The project operates none
of those servers and has no visibility into that traffic.

[Your data on this machine](#your-data-on-this-machine) documents the files
DBFlux writes, what the audit log records, how secrets are stored, and how to
back up or fully reset the application.

### Telemetry (optional)

DBFlux ships with an optional telemetry module. It is a compiled-in feature
that can be excluded entirely with `--no-default-features`, like the other
default features. Events go to `https://telemetry.dbflux.dev`. Nothing is sent
and no usage is counted until you confirm the visibly presented Welcome
disclosure. Hiding it programmatically is not confirmation.

Basic is preselected in the Welcome dialog, so confirming without changing the
selection applies Basic; choose Minimal or Off there to send less. If you had
disabled the Welcome dialog, it opens once after updating so you can choose,
and it does not open again for this reason.

| Level | What it sends |
|-------|---------------|
| Off | Nothing: no ping, no activity event and no usage report. Nothing is queued, and choosing Off clears every pending delivery and the usage counters. |
| Minimal | A ping once per published build, plus one activity event per week: random installation identifier, app version, channel, OS and architecture. No system language, country or usage counts. |
| Basic (preselected) | Minimal's ping, plus a weekly report instead of the activity event. The report adds system language (not the UI language), country (see below) and bounded counts of supported successful operations in the six categories below. |

**Country.** The app sends no country; it sends the placeholder `ZZ`. For Basic
reports only, the collector stores the coarse country that Cloudflare resolves
at its edge. The collector never stores the IP address. Cloudflare and Fly.io,
which carry the request, keep their own request logs.

**Retention.** The collector keeps raw events, including the installation
identifier, for 90 days after receipt. After that only identifier-free counts
remain.

**Turning it off.** Choose Off in the Welcome dialog or in Settings › Updates &
privacy. To disable delivery without opening the app, for example on managed
machines, set the environment variable `DBFLUX_TELEMETRY=off`; it wins over
every level and endpoint. A build made without the `telemetry` feature
contains no telemetry code at all.

#### Which operations count

Counts measure whole operations, not rows or performance. A successful operation
with zero rows can count. Background refreshes, partial failures, errors and
cancellations do not count. Basic and the disclosure confirmation must already
be active when work starts; an unavailable source or disabled/degraded Audit
prevents credit.

| Category | Coverage |
|----------|----------|
| `query_run` | Explicit user query runs from the toolbar, commands or gutter; not automatic refreshes or derived Explain runs. |
| `visual_query_builder` | Actual user Run with full execution success. Queuing or approving a request is not execution and does not count. |
| `dashboard_open` | Direct sidebar opens or refocuses with valid dashboard metadata; not implicit creation, import or session restoration. |
| `script_run` | Explicit editor script runs with canonical exit code 0, without timeout or detachment. |
| `mcp_tool` | Only reviewed, supported successful primary tool operations through an authenticated, running GUI foreground owner; see the limits below. |
| `schema_inspect` | Explicit focused or global inspection that newly loads complete required metadata to Ready; not focusing an existing tab. A deliberate bounded scope of up to 100 tables can count; failed required metadata cannot, but fully recovered metadata can. |

**MCP coverage is limited, not a count of every tool call.** A standalone server
without the running GUI owner does not count. Unknown tools, help/protocol calls,
approval controls and pending approvals do not count. `count_records`,
`insert_record` and `list_scripts` are excluded even when successful because their
current APIs do not preserve sufficient completion evidence. `update_records`
and `delete_records` require the primary mutation's typed `QueryResult` completion;
an opaque successful `DocumentCrudResult` does not qualify. Proven typed SQL
update, upsert and delete completions can count, including zero affected rows.
These exclusions do not change whether the tools themselves succeed.

For supported MCP operations, permission is acquired after authorization allows
work and before it starts. The counter is credited only after the completed
primary operation and a successful execution audit, not at authorization. This
uses existing local authentication, not a new trusted-client identity guarantee.
Cancellation can leave a bounded permission lease until a future foreground
expiry pass (24-hour cap); cleanup is not immediate, and arbitrarily long calls
are not guaranteed coverage. Tool names in this explanation are local coverage
details, never telemetry payload fields.

Facts that do not change with the level:

- The installation identifier is a random UUID created on first run and kept
  for the life of the installation: it never rotates on a schedule. It is
  **pseudonymous, not anonymous**: it identifies an installation, not a person
  or a unique device. Reports also have a delivery UUID, which is unique per
  report. Resetting the identifier in Settings destroys the old one and mints a
  new random one; nothing links the two.
- Never sent: operation, query, client or person identifiers, tool names,
  queries, results, credentials, file paths, connection metadata, cross-process
  actor data or unrestricted error text. There are no Extended reports, public
  statistics, per-engine counts, error reports or performance timings.
- Every delivery attempt records the full correlated payload, level and outcome
  in the local [audit log](#audit-and-privacy). Readable display labels do not
  change raw storage, copy, export or wire contents. Events go only to the
  project's collector at `https://telemetry.dbflux.dev` to help guide development.
- Resetting the installation id in Settings › Updates & privacy replaces the
  identifier at once and cancels pending sends; already-acknowledged builds
  stay acknowledged.

The level and the reports live in Settings › Updates & privacy. The staged
level applies when you save; resetting to defaults stages Basic without
touching the installation id. Choosing Basic or saving does not promise an
immediate network report. Leaving Basic clears the pending Basic report and
the counters, and Off also clears a pending ping; a delivery already handed to
the network may still complete. Acknowledging a sent snapshot preserves newer
increments. A reset invalidates old in-flight usage work while preserving
acknowledged-build history.

## Your data on this machine

Where DBFlux stores your data, how it protects your credentials, what the audit
log keeps, and how to back up or fully reset.

### At a glance

| Your data | Where it lives |
|-----------|----------------|
| Connection profiles, settings, history, saved charts/queries, audit log | One SQLite file: `dbflux.db` in the data directory |
| Open tabs / session | The same `dbflux.db`, plus script files and scratch/shadow files on disk |
| Passwords, passphrases, API secrets | Your **OS keyring** — never in `dbflux.db` |
| IPC/MCP auth token | A `0600` file in the config directory |

DBFlux keeps almost everything in a single SQLite database. Secrets are the
deliberate exception: they go to the operating system's keyring, and the database
only stores a *reference* to them.

### Data locations

DBFlux uses your platform's standard directories.

| Platform | Data directory | Config directory |
|----------|----------------|------------------|
| **Linux** | `~/.local/share/dbflux/` | `~/.config/dbflux/` |
| **macOS** | `~/Library/Application Support/dbflux/` | `~/Library/Application Support/dbflux/` |
| **Windows** | `%APPDATA%\dbflux\` | `%APPDATA%\dbflux\` |

The data directory holds:

- **`dbflux.db`** — the unified database (everything below in [What's in the
  database](#whats-in-the-database)).
- **`sessions/`** — scratch and shadow files for open editor tabs: untitled
  content, plus a recovery copy of unsaved edits.
- **`ipc_auth_token`** — the IPC/MCP auth token (see [below](#ipcmcp-auth-token)).
- **`ssh_known_hosts`** — accepted SSH host keys (TOFU).

DBFlux no longer uses the config directory. Older versions stored the IPC auth
token and SSH known-hosts there; leftover files may remain after upgrading and
can be removed.

#### Stable vs. Nightly

A Nightly build uses a separate database file, `dbflux-nightly.db`, so a
pre-release migration can never touch your stable data. Stable and release
candidate builds both use `dbflux.db`.

You can make a Nightly build share the stable database via **Settings → General →
Storage → Use the stable database** (applies on next launch). Internally this
just drops an empty `use-stable-db` marker file in the data directory.

### What's in the database

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

### Secrets and the OS keyring

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

#### When secrets are (and aren't) saved

- A connection password is only stored when you tick **Save password**; SSH and
  proxy secrets only when you tick their **Save** checkbox.
- If no keyring is available, DBFlux hides the **Save** checkboxes and does not
  persist secrets — you re-enter them each session.
- A *locked* keyring still counts as available: writes may fail until you unlock
  it, but DBFlux keeps secret support enabled.

### Session and tabs restore

Which tabs you have open — their kind, file paths, order, active tab, and pin
state — is recorded in `dbflux.db` (`st_sessions` / `st_session_tabs`). The
scratch/shadow copies used to restore them live under `sessions/` in the data
directory. A script backed by a file is saved to that file itself: on the
auto-save interval, when you close its tab, and when you quit. Unsaved untitled
content is kept in the sessions folder. Those automatic writes never overwrite
a script file that changed outside dbflux: such a write is refused, and the
pending edits stay in the editor (and in the sessions-folder copy). `Ctrl+s`
and **Save File As** are deliberate: they write the file as you asked. On
startup DBFlux
restores this session when **Settings → General → Restore session on startup**
is on (the default).

### Audit and privacy

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
[Audit](docs/AUDIT.md), including its [Audit viewer](docs/AUDIT.md#audit-viewer)
section.

### IPC/MCP auth token

DBFlux exposes a local IPC surface (used by the MCP server and external RPC
services). It authenticates callers with a token stored at:

```
<data dir>/dbflux/ipc_auth_token
```

(on Linux, `~/.local/share/dbflux/ipc_auth_token`). It's a random value regenerated on
each startup, written with owner-only `0600` permissions, and also exported to the
`DBFLUX_IPC_TOKEN`, `DBFLUX_DRIVER_IPC_TOKEN`, and `DBFLUX_AUTH_PROVIDER_IPC_TOKEN`
environment variables for child processes.

This token is **process-identity only** — any local process that can read it can
connect. Do not expose the IPC/MCP surface beyond localhost without an additional
authentication layer. See [AI + MCP Integration](docs/MCP_AI_INTEGRATION.md) for the
trust model.

### Backup and reset

DBFlux has no dedicated backup/restore command, but because everything lives in
one file, both are straightforward.

#### Back up

Copy the single database file while DBFlux is closed:

```
~/.local/share/dbflux/dbflux.db        # Linux (adjust per platform)
```

That file contains your profiles, history, saved charts/queries, and audit log.
Your **secrets are not in it** — they stay in the OS keyring — so a copied
database on another machine will reference keyring entries that don't exist there
until you re-enter the secrets.

#### Full reset

To wipe DBFlux's data:

1. Delete the **data directory** (`~/.local/share/dbflux/` on Linux) — removes the
   database, session files, IPC auth token, and SSH known-hosts.
2. **Older versions only:** delete the legacy config directory
   (`~/.config/dbflux/` on Linux) if it still exists — current versions no longer
   use it.
3. **Clear keyring entries manually.** Secrets under the `dbflux` service remain
   in your OS keyring after deleting the directories; remove them with your
   platform's keyring tool if you want a complete wipe.

> Deleting the data directory is irreversible. Back up `dbflux.db` first if you
> might want your profiles or history back.

## The website and documentation

`dbflux.dev` and `docs.dbflux.dev` are static sites built from this
repository. They:

- set no cookies, first-party or third-party, and store only one thing in your
  browser: the theme you pick (Auto, Light, or Dark), which stays in local
  storage and is never sent anywhere;
- load no third-party analytics, advertising, or tracking script;
- serve their fonts, assets, and scripts from the same host, so a page view
  contacts no other domain;
- keep no server-side log the project can read per visitor.

The documentation search runs in your browser against an index file fetched
from the same host. The query never leaves the page.

## Website usage measurement

> **Status: not active.** This section describes the measurement before it
> ships. The version that enables it announces the change in its release notes,
> and this line changes in the same commit.

To learn which pages people read and how fast they load, each page view sends
a small anonymous report. The script, the collector, and the schema that
defines every accepted field live in this repository.

### What a report contains

| Field | Example | Where it comes from |
|-------|---------|---------------------|
| Host | `site` or `docs` | The page |
| Page | The route, such as `/docs/usage/`. Never the query string or fragment. | The page, checked against the list of pages in the build |
| Documentation version and page language | `v0.7`, `es` | The page |
| Browser language | `es`, the primary language only | The browser |
| Screen class | `mobile`, `tablet`, or `desktop` | The browser |
| Origin of the visit | `none`, `internal`, `search`, `github`, `social`, or `other`. Never the address. | The browser |
| Load timing | Time to first byte, DOM ready, full load, and largest contentful paint, in milliseconds | The browser |
| Time on page | Visible time only, in milliseconds | The browser |
| Reading depth | Furthest scroll position in 25% steps, and visible time per section heading | The browser |
| Country | Two-letter code, such as `AR` | Cloudflare, from the request, before forwarding |
| Browser and operating system | Family and major version, such as Firefox 131 on Linux | Derived from the User-Agent before forwarding; the full string is discarded |

Every text field accepts only a fixed list of values or a strict pattern. The
collector rejects anything else, so a report cannot carry free text.

### What is never collected

- The IP address. The Cloudflare Worker that receives a report builds a new
  request that contains only the fields above, so the project's server never
  receives the visitor's address.
- The full User-Agent string, the full referrer address, query strings, or
  fragments.
- Cookies, browser storage, fingerprints, or identifiers of any kind: the
  measurement neither reads nor writes them. Two reports from the same person
  cannot be linked, and the project does not count unique visitors.
- Mouse movement, clicks, keystrokes, form input, or page content.

### How to opt out

The script sends nothing when the browser signals Global Privacy Control or Do
Not Track, and the Worker drops any report that arrives with either signal.
Blocking the script with a content blocker also works; pages behave the same
without it.

### Where the data lives

Reports are stored on a server the project rents from OVHcloud in the European
Union, not in an analytics service. Raw reports are kept for 90 days; the daily
totals derived from them are kept indefinitely. Only the maintainer reads them,
over a private network the project runs itself. Backups are encrypted before
they leave the server and are stored in Cloudflare R2, which holds the
encrypted copy but not the key.

### Removing data

A report contains nothing that identifies a person, so there is no record to
look up for a given visitor. If you believe a report carried something it
should not have, report it privately through
[GitHub Security Advisories](https://github.com/0xErwin1/dbflux/security/advisories/new)
rather than in a public issue. The affected data is removed from storage and
from the backups that contain it.

## Infrastructure providers

These services sit between the project and you, or hold data on the project's
behalf. None is used to identify individual visitors.

| Provider | Role | What it sees |
|----------|------|--------------|
| Cloudflare | Hosts the website, the documentation, and the documentation MCP endpoint at `mcp.dbflux.dev`. Runs the Worker that forwards usage reports, and stores the encrypted backups of those reports in R2. | Every HTTP request to those hosts, including the IP address and user agent, as any host does, and the contents of a usage report while forwarding it. Cloudflare exposes aggregate traffic counts to the project. It does not expose per-visitor records, and the project has enabled no feature that would. In R2 it holds only encrypted backups. |
| OVHcloud | Hosts the server that stores website usage reports, in a data center in the European Union. | The stored reports, as the operator of the machine they live on. It never receives an IP address or User-Agent from a visitor, because reports reach the server without them. It has no role in the application. |
| Google Search Console | Reports how the site appears in Google search results. | Only what Google's crawler and search results already know. No script from Google is loaded on any page. |

Cloudflare's own handling of request data is described in the
[Cloudflare privacy policy](https://www.cloudflare.com/privacypolicy/).

## The documentation MCP endpoint

`mcp.dbflux.dev` lets an AI client search the documentation. A session lives
for the duration of one connection and holds only the messages exchanged in
it. There are no accounts, no persistent storage of queries, and no record
linking a session to a visitor after it ends.

## GitHub

Issues, pull requests, discussions, and release downloads are on GitHub.
Anything you post there is public and governed by
[GitHub's privacy statement](https://docs.github.com/site-policy/privacy-policies/github-general-privacy-statement).

## Changes to this policy

This file is versioned with the source code. The commit history of
`PRIVACY.md` is the change log. A change that makes the application or the
website collect anything it does not collect today will be announced in the
release notes of the version that introduces it.

## Contact

Questions go to an issue in this repository with the title prefix
`[privacy]`. To report data that should not be public, use
[GitHub Security Advisories](https://github.com/0xErwin1/dbflux/security/advisories/new)
instead.

## Related

- [Settings & Hooks](docs/SETTINGS.md) — the General/Audit/Storage controls referenced here.
- [Connecting → Advanced Setup](docs/CONNECTIONS.md) — where secrets are entered.
- [Audit](docs/AUDIT.md) — the full audit event schema and redaction details.
