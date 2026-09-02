# DBFlux Usage Guide

A practical, end-user introduction to working with DBFlux: connecting to a
database, browsing its schema, running queries, working with results, charting,
and the keyboard model.

DBFlux is keyboard-first. Almost every action has both a mouse affordance and a
keyboard binding. The keybindings listed in this guide are the application
defaults; you can review the full active keymap in **Settings → Keybindings**
(a read-only viewer — see the [Settings overview](#8-settings-overview)).

---

## 1. First Launch and Creating a Connection

On startup DBFlux restores your previous session (open tabs). On a fresh
install there is nothing to restore, so focus defaults to the sidebar.

### Opening the Connection Manager

Open the Connection Manager to create or edit connections:

- From the sidebar, press `c`.
- Or use the command palette (`Ctrl+Shift+P` / `Cmd+Shift+P` on macOS) and run
  **Open Connection Manager**.

### Choosing a driver

The Connection Manager presents a driver picker. Available drivers depend on the
features the binary was built with; the standard build includes SQLite,
PostgreSQL, MySQL/MariaDB, MongoDB, Redis, DynamoDB, Microsoft SQL Server, and
AWS-backed integrations. Externally registered RPC drivers also appear here when
configured (see `docs/RPC_SERVICES_CONFIG.md`).

Use `/` to filter the driver list, `j`/`k` (or arrow keys) to move, and `Enter`
to select.

### Form mode vs. direct URI

Each driver provides its own connection form. The form is dynamic: it shows only
the fields that driver actually needs. Most relational drivers support two ways
to supply connection details:

- **Form-based**: individual fields (host, port, database, user, etc.).
- **Direct URI**: a single connection-string field.

File-backed drivers such as SQLite use a file-path form instead.

### Access tab: direct, SSH, proxy, managed

The **Access** tab controls how DBFlux reaches the database:

- **Direct** — connect straight to the host from the Main tab fields. Direct
  mode can still resolve Secret/Parameter/Auth value sources for individual
  fields.
- **SSH** — tunnel the connection through an SSH host. SSH tunnel profiles are
  managed centrally in Settings and selected per connection.
- **Proxy** — route through a SOCKS5 or HTTP CONNECT proxy. Proxy and SSH are
  mutually exclusive for a single connection.
- **Managed** — provider-managed access (for example `aws-ssm`), where DBFlux
  opens access through an external provider before connecting.

When you connect, DBFlux runs a pre-connect pipeline: authentication and session
validation, dynamic value resolution, then managed/direct access setup, followed
by the driver connect and an initial schema fetch. Connection hooks (if
configured) run at the PreConnect, PostConnect, PreDisconnect, and PostDisconnect
phases. See the Settings overview for where hooks are defined.

---

## 2. Browsing the Schema

The sidebar has two tabs:

- **Connections** — the schema tree (databases, schemas, tables/collections,
  columns, indexes, and — where the driver supports it — a Routines folder).
- **Scripts** — file and folder management for saved query files, script hooks,
  and other user files.

Switch between the two tabs with `q` or `e`.

### Navigating the tree

- `j`/`k` (or `Down`/`Up`) — move the selection.
- `h` collapses, `l` expands the current node. `Space` toggles expand/collapse.
- `g` jumps to the first item, `Shift+g` to the last; `Home`/`End` do the same.
- `Ctrl+d`/`Ctrl+u` (or `PageDown`/`PageUp`) — page through long lists.
- `/` focuses the sidebar search/filter.
- `Enter` opens the selected item (for example, a table opens a data grid).
- `r` refreshes the schema; `d` disconnects the active connection.
- `m` opens the context menu for the selected item.

### Lazy loading

Schema is loaded lazily. On connect, DBFlux fetches shallow metadata (names).
Detailed metadata — columns, indexes, and similar — is fetched on demand when you
expand a node. This keeps the initial connection fast on large databases.

### Routines / stored procedures

For drivers that advertise routine support (PostgreSQL is the first
implementation), the schema tree includes a **Routines** folder containing
functions, procedures, aggregates, and window routines. Opening a routine opens a
read-only code document showing its definition. The document is non-editable but
you can still select and copy its text; execution and mutation controls are
hidden.

---

## 3. Running Queries

Open a new query tab with `Ctrl+n` (`Cmd+n` on macOS), or open a script file with
`Ctrl+o`. The editor's query language (SQL, MongoDB query syntax, Redis commands,
etc.) is determined by the active connection's driver, which also drives syntax
highlighting and the placeholder text.

### Executing

- `Ctrl+Enter` (`Cmd+Enter`) — **Run Query**.
- `Ctrl+Shift+Enter` (`Cmd+Shift+Enter`) — **Run Query in New Tab**.

If a non-empty text selection exists, only the selected text runs. With no
selection, the full editor buffer is used.

### Multi-statement scripts

When you run with no selection and the buffer contains multiple `;`-separated
statements, and the active driver advertises batch support, DBFlux shows a
confirmation dialog (`Run entire script (N statements)?`) before executing. On
confirmation each statement's result set is rendered in its own result tab.

Statement splitting is language-aware for SQL-family languages: separators inside
strings, identifiers, line/block comments, and PostgreSQL dollar-quoted bodies are
not treated as statement boundaries. Non-SQL languages remain single-statement.
Batch support is per-driver — among the built-in SQL drivers, PostgreSQL,
MySQL/MariaDB, SQLite, and Microsoft SQL Server support it. A selection always
runs as-is and never triggers the script confirmation.

### Dangerous-query confirmation

DBFlux detects dangerous operations across languages — SQL `DELETE`/`DROP`/
`TRUNCATE` and `DELETE`/`UPDATE` without a `WHERE`, MongoDB `deleteMany`/`drop`,
Redis `FLUSHALL`/`FLUSHDB`/`KEYS` — and prompts for confirmation before running.
This behavior is governed by settings: dangerous-query confirmation can be turned
off, a `WHERE` clause can be required for `DELETE`/`UPDATE`, and Redis
`FLUSHALL`/`FLUSHDB` can be disabled entirely (in which case those commands are
blocked rather than confirmed).

### Scripts (Lua / Python / Bash)

Lua, Python, and Bash documents execute as scripts rather than database queries.
Their output streams live into the document's output area while running, and the
final output is kept as a text result. See `docs/LUA.md` for the embedded Lua
runtime.

### Visual query builder

For SQL connections you can compose queries without writing SQL. From a table's
data grid toolbar, click **Builder** to open a right-rail panel. The builder is
available only on SQL drivers; non-SQL connections do not show it.

The panel has a mode selector at the top — **SELECT**, **UPDATE**, **DELETE** —
and a live SQL preview that regenerates on every change. The preview is always
visible. Press **Run** to execute, or (in SELECT mode) **Open in Editor** to drop
the generated SQL into a normal query editor. The header has **Save** and
**Reset**.

| Keys | Action |
|------|--------|
| `Cmd+Enter` / `Ctrl+Enter` | Run |
| `Cmd+E` / `Ctrl+E` | Open in Editor (SELECT mode) |
| `Cmd+S` / `Ctrl+S` | Save |
| `Cmd+Shift+S` / `Ctrl+Shift+S` | Save As |
| `Cmd+Backspace` / `Ctrl+Backspace` | Reset |

#### Building a SELECT

The SELECT body has sections you fill in top to bottom:

- **Columns** — the projection (which columns to select).
- **Filters** — a `WHERE` predicate tree. Predicates can be nested into AND/OR
  groups, so you can build complex conditions visually.
- **Joins** — additional tables with an alias and an `ON` condition.
- **Group By / Aggregates** — see below.
- **Sort** — `ORDER BY` entries.
- **Limit & Offset** — paging bounds.

The SQL preview is parameterized: literal values are emitted as placeholders for
the active dialect (SQLite, PostgreSQL, MySQL/MariaDB, or SQL Server).

#### GROUP BY and aggregates

Add group columns and aggregates in the **Group By / Aggregates** section. The
supported aggregate functions are `COUNT`, `COUNT(*)`, `COUNT(DISTINCT)`, `SUM`,
`AVG`, `MIN`, and `MAX`. Each aggregate gets an editable alias that
auto-generates from the function and column.

Once the query is grouped:

- The **Columns** section is replaced by a read-only preview of the effective
  `SELECT` (group columns followed by aggregate aliases).
- A **Having** section appears, using the same predicate editor as Filters but
  applied to `HAVING`.
- **Sort** entries are restricted to group columns and aggregate aliases;
  invalid entries are rejected with a visible error.

How grouped results behave in the data grid is described under
[Aggregated results](#aggregated-results).

#### Schema-aware autocomplete

The builder's single-line inputs (filter, sort, projected columns, the join
target table, and both sides of a join `ON`) offer inline suggestions sourced
from the live schema and the builder's own spec: source-table columns, declared
join aliases, and joined-table columns (fetched lazily in the background). Typing
`<alias>.` scopes suggestions to that alias's columns only. Matching is
prefix-only.

| Keys | Action |
|------|--------|
| `Up` / `Down` | Move through suggestions |
| `Tab` / `Enter` | Commit the highlighted suggestion |
| `Esc` (or focus loss) | Dismiss |

The same autocomplete is available in the data grid's `WHERE` filter input (see
[Filtering results](#filtering-results)).

#### Saved queries

Builders can be saved per connection profile and reopened later. Saved queries
are scoped to the profile, with unique names. A saved query can also be imported
onto a different connection; on import DBFlux verifies that the referenced tables
exist on the target connection before loading.

#### Visual UPDATE and DELETE

Switch the mode selector to **UPDATE** or **DELETE** to build a mutation. Both
modes reuse the same filter editor for the `WHERE` clause; UPDATE adds an
assignments section for the `SET` columns (including raw-expression assignments).
The SQL preview stays visible the whole time.

Mutations are subject to a policy that composes the connection's read-only state
and the actor context:

| Policy | Effect |
|--------|--------|
| Allowed | The mutation can run. |
| Read-only | Execution is blocked (for example, a read-only profile). |
| Approval required | The mutation must be approved before it runs. |

**Execution mode.** The **Execution** section offers three modes, with a default
auto-suggested from the row-count estimate, the driver's transaction support, and
primary-key availability. Overriding the suggestion shows a tradeoff modal.

| Mode | Behavior |
|------|----------|
| **Single TX** | One transaction for the whole change. |
| **Chunked TX** | Keyset-paginated chunks over the table's primary key (chunk size clamped to 1000–10000, default 5000). Each chunk is its own transaction, surfaces a Tasks-panel entry, can be cancelled between chunks, and rolls back on failure. |
| **Direct** | No transaction wrapper (autocommit). Used when the driver does not support transactions. |

**Dangerous-query gate.** An `UPDATE` or `DELETE` with no `WHERE` is gated by the
dangerous-query confirmation (see
[Dangerous-query confirmation](#dangerous-query-confirmation)) before it runs.

---

## 4. Working with Results

Results render in result tabs inside the document. The view mode is chosen
automatically from the database category:

- **Table view** for relational databases.
- **Document tree view** for document databases (for example MongoDB, DynamoDB).
- **Key-value view** for Redis.

Event-stream-style containers open as event streams when the driver declares that
presentation.

### Navigating the data grid

When the results panel has focus:

- `j`/`k` (or `Down`/`Up`) — move between rows.
- `h`/`l` (or `Left`/`Right`) — move between columns.
- `g`/`Shift+g` (or `Home`/`End`) — first / last row.
- `Ctrl+d`/`Ctrl+u` (or `PageDown`/`PageUp`) — page through rows.
- `[` / `]` — previous / next page of results (pagination).
- `f` focuses the toolbar; `/` focuses the search/filter.
- `z` toggles collapsing the panel.
- `m` (or `Shift+F10`) opens the row/cell context menu.

### Filtering results

The data grid toolbar has a `WHERE` filter input that re-runs the query with the
condition you type. For SQL connections it supports two styles:

- **Raw `WHERE`** — type a plain condition (for example `status = 'active'`). This
  is the default behavior.
- **Relational (ORM-style) paths** — type a dotted path that walks foreign keys,
  for example `created_by.email LIKE '%@acme.com'` or
  `created_by.organization.name = 'Acme'`. DBFlux resolves the path against the
  table's foreign-key metadata and joins through to the referenced table for you;
  there is no need to write the JOINs by hand.

When a relational filter resolves, a chip shows how many joins it added. If a
segment is ambiguous or cannot be resolved, an inline error appears with an
**Open in builder** link that opens the visual query builder seeded with the
joins resolved so far. Non-dotted input always keeps the raw-`WHERE` behavior.

The filter input also offers schema-aware autocomplete (same navigation as the
builder — see [Schema-aware autocomplete](#schema-aware-autocomplete)).

### Editing and CRUD

In the data grid:

- `o` — add a row.
- `x` — delete the selected row.
- `r` — rename / edit (context-dependent).
- `y` — copy the selected row.
- `Ctrl+c` (`Cmd+c`) — copy the selected cell(s) to the clipboard.

#### When results are editable

Plain table browses are editable when the table has a primary key. Results
produced by the **visual query builder** (SELECT mode) are also editable, but
only when they are provably bound to a single table: the result maps 1:1 to one
underlying table and every primary-key column of that table is projected under
its original name. Edits and deletes then build their `WHERE` from the projected
primary-key values.

JOINs are allowed: columns from the source table are editable, while joined
columns are read-only.

A builder result falls back to **read-only** — with a toolbar hint explaining why
— when any of these hold:

- The query aggregates or uses `GROUP BY` / `HAVING`.
- The projection is a wildcard across a JOIN.
- A primary-key column is missing or projected under an alias.
- The table's keys have not been loaded from the schema cache yet (the grid
  upgrades itself to editable once the keys arrive).

Free-form SQL typed into the editor stays read-only; inline edit applies only to
plain table browses and builder-generated SELECTs.

#### Aggregated results

When a result comes from a grouped (`GROUP BY`) query, rows show the aggregated
output and editing is disabled — add-row, delete-row, edit-cell, and inspect-row
are unavailable, with explanatory tooltips. Pagination counts the grouped rows
(not the underlying rows), so the page total is accurate. Aggregate columns keep
the correct column kind, so charting still works.

### Copy as Query

The result context menu includes **Copy as Query**, which generates a
driver-specific mutation statement (or envelope, for non-SQL drivers) from the
selected row using the driver's own query generator.

### Exporting

Press `Ctrl+e` (`Cmd+e`) in the results panel, or run **Export Results** from the
command palette. The available formats depend on the result shape and include:

- **CSV**
- **JSON (pretty)** and **JSON (compact)**
- **Text**
- **Binary** (for binary-shaped results)

---

## 5. Charting Results

Any query that produces tabular results can be charted. In the query editor
toolbar, click the chart button (tooltip: "Open current query in a chart
document") to open the current query in a chart document.

Charts use the column kind metadata supplied by the driver to auto-detect axes
(time columns, numeric columns, and so on). The supported chart kinds are:

- **Line**
- **Bar**
- **Scatter**
- **Area**
- **Stacked Bar**
- **Pie**

Charts can be saved per connection profile. To reopen a saved chart, run **Open
Chart...** from the command palette (`OpenSavedChart`), which lists the saved
charts for the current profile in a fuzzy overlay.

---

## 6. Saved Queries and History

DBFlux keeps a history of completed queries and lets you save named queries.

- `Alt+h` (in the editor) toggles the query history dropdown.
- `Ctrl+s` (`Cmd+s`) — **Save** the current query.
- `Ctrl+Shift+s` (`Cmd+Shift+s`) — **Save File As**.
- `Ctrl+p` (`Cmd+p`, in the editor) — open the saved-queries browser.

Inside the history modal you can navigate with `Ctrl+j`/`Ctrl+k` (or arrow keys),
open an entry with `Enter`, and use the local mnemonics `Ctrl+f` (toggle
favorite), `Ctrl+r` (rename), and `Ctrl+d` (delete). `/` focuses the modal
search.

---

## 7. Keyboard Reference

DBFlux uses a layered, context-aware keymap. The active layer depends on which
panel has focus. Bindings written with the **primary** modifier use `Cmd` on
macOS and `Ctrl` on every other platform; bindings written with literal `Ctrl`
stay `Ctrl` on all platforms (to avoid clashing with macOS system shortcuts).

### Global (available regardless of focus)

| Keys | Action |
|------|--------|
| `Ctrl+Shift+P` / `Cmd+Shift+P` | Toggle command palette |
| `Ctrl+n` / `Cmd+n` | New query tab |
| `Ctrl+w` / `Cmd+w` | Close current tab |
| `Ctrl+Tab` / `Ctrl+Shift+Tab` | Next / previous tab |
| `Ctrl+1` .. `Ctrl+9` / `Cmd+1` .. `Cmd+9` | Switch to tab N |
| `Ctrl+o` / `Cmd+o` | Open script file |
| `Ctrl+Enter` / `Cmd+Enter` | Run query |
| `Ctrl+Shift+Enter` / `Cmd+Shift+Enter` | Run query in new tab |
| `Escape` | Cancel / close modal |
| `Tab` / `Shift+Tab` | Cycle focus forward / backward |
| `Ctrl+Shift+1` | Focus sidebar |
| `Ctrl+Shift+2` | Focus editor |
| `Ctrl+Shift+3` | Focus results |
| `Ctrl+Shift+4` | Focus background tasks |
| `Ctrl+Shift+A` / `Cmd+Shift+A` | Open audit viewer |
| `Ctrl+b` / `Cmd+b` | Toggle sidebar |
| `Ctrl+m` | Open tab context menu |

### Sidebar

| Keys | Action |
|------|--------|
| `q` / `e` | Switch sidebar tab (Connections / Scripts) |
| `/` | Focus search |
| `j` / `k` (or `Down` / `Up`) | Select next / previous |
| `h` / `l` | Collapse / expand node |
| `Space` | Expand / collapse |
| `g` / `Shift+g` (or `Home` / `End`) | First / last item |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Page down / up |
| `Enter` | Open / execute item |
| `r` | Refresh schema |
| `c` | Open Connection Manager |
| `d` | Disconnect |
| `m` | Open item menu |
| `Shift+j` / `Shift+k` | Extend selection down / up |
| `Space` (with Shift) | Toggle selection |
| `Ctrl+j` / `Ctrl+k` | Move selected item down / up |
| `Shift+r` | Rename |
| `x` | Delete |
| `Shift+n` | Create folder |
| `Ctrl+l` | Focus panel to the right |

### Editor

| Keys | Action |
|------|--------|
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` | Focus left / down / up panel |
| `Alt+h` | Toggle history dropdown |
| `Ctrl+p` / `Cmd+p` | Open saved queries |
| `Ctrl+s` / `Cmd+s` | Save query |
| `Ctrl+Shift+s` / `Cmd+Shift+s` | Save file as |
| `Enter` | Focus / execute |

(Unmodified letters are intentionally left to the text input so typing works.)

### Results

| Keys | Action |
|------|--------|
| `Ctrl+h` / `Ctrl+k` / `Ctrl+l` | Focus left / up / right panel |
| `Ctrl+j` | Focus toolbar |
| `j` / `k` (or `Down` / `Up`) | Next / previous row |
| `h` / `l` (or `Left` / `Right`) | Column left / right |
| `g` / `Shift+g` (or `Home` / `End`) | First / last row |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Page down / up |
| `]` / `[` | Next / previous results page |
| `Ctrl+e` / `Cmd+e` | Export results |
| `f` | Focus toolbar |
| `/` | Focus search/filter |
| `x` | Delete row |
| `r` | Rename / edit |
| `o` | Add row |
| `y` | Copy row |
| `Ctrl+c` / `Cmd+c` | Copy cell(s) |
| `z` | Toggle panel collapse |
| `m` (or `Shift+F10`) | Open context menu |

### Background Tasks

| Keys | Action |
|------|--------|
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` | Focus left / down / up panel |
| `j` / `k` (or `Down` / `Up`) | Select next / previous |
| `g` / `Shift+g` (or `Home` / `End`) | First / last |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Page down / up |
| `z` | Toggle panel collapse |

### Command palette

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Select next / previous |
| `Enter` | Execute |
| `Escape` | Cancel |

### Context menu

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Move down / up |
| `Enter` / `l` (or `Right`) | Select / enter submenu |
| `Escape` / `h` (or `Left`) | Back / close |

### History modal

| Keys | Action |
|------|--------|
| `Ctrl+j` / `Ctrl+k` (or `Down` / `Up`) | Select next / previous |
| `Enter` | Open entry |
| `Ctrl+f` | Toggle favorite |
| `Ctrl+r` | Rename |
| `Ctrl+d` | Delete |
| `/` | Focus search |
| `Ctrl+s` / `Cmd+s` | Save query |

---

## 8. Settings Overview

Settings has these sections (the MCP sections appear only in builds with AI/MCP
support, which is the default):

- **General** — application-wide preferences: theme, startup/session, refresh
  defaults, and the dangerous-query confirmation behavior.
- **Audit** — what the audit log captures (log-capture minimum level) and
  retention.
- **MCP Clients / Roles / Policies** — AI client governance (trusted clients,
  roles, policies). See `docs/MCP_AI_INTEGRATION.md`.
- **Keybindings** — a **read-only** viewer for the active keymap, with a text
  filter and conflict warnings. Rebinding from the UI is not available.
- **Proxies** — SOCKS5 / HTTP CONNECT proxy profiles.
- **SSH Tunnels** — SSH tunnel profiles selectable per connection.
- **Auth Profiles** — provider-driven authentication profiles (AWS SSO / shared
  credentials).
- **Services** — externally registered RPC services (drivers and auth
  providers). See `docs/RPC_SERVICES_CONFIG.md`.
- **Hooks** — global connection-hook definitions (Command, Script, and Lua
  modes). Per-profile phase bindings live in the Connection Manager's Hooks tab.
- **Drivers** — per-driver overrides and settings.
- **About** — version and build information.

For a full per-setting reference and the connection-hooks guide, see
`docs/SETTINGS.md`.

### Related documentation

- Advanced connection setup (SSH, proxy, AWS SSO, value sources):
  `docs/CONNECTIONS.md`
- Full Settings reference and connection hooks: `docs/SETTINGS.md`
- Data storage and privacy (where data and secrets live, backup, reset):
  `docs/DATA_AND_PRIVACY.md`
- Dashboards, saved charts, and the audit viewer (usage): `docs/DASHBOARDS_AND_AUDIT.md`
- Connection hooks and the embedded Lua runtime: `docs/LUA.md`
- AI client integration (MCP): `docs/MCP_AI_INTEGRATION.md`
- Audit log and event schema: `docs/AUDIT.md`
- External RPC drivers/services: `docs/RPC_SERVICES_CONFIG.md`,
  `docs/DRIVER_RPC_PROTOCOL.md`
