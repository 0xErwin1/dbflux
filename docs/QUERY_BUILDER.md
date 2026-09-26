# Visual Query Builder

For SQL connections you can compose queries without writing SQL. From a table's
data grid toolbar, click **Builder** to open a right-rail panel. The builder is
available only on SQL drivers; non-SQL connections do not show it.

The panel has a mode selector at the top — **SELECT**, **UPDATE**, **DELETE** —
and a live SQL preview that regenerates on every change. The preview is always
visible. Press **Run** to execute, or (in SELECT mode) **Open in Editor** to drop
the generated SQL into a normal query editor. The header has **Save**,
**Reset**, and a close button that hides the rail and keeps what you built;
**Builder** brings it back.

| Keys | Action |
|------|--------|
| `Cmd+Enter` / `Ctrl+Enter` | Run |
| `Cmd+E` / `Ctrl+E` | Open in Editor (SELECT mode) |
| `Cmd+S` / `Ctrl+S` | Save |
| `Cmd+Shift+S` / `Ctrl+Shift+S` | Save As |
| `Cmd+Backspace` / `Ctrl+Backspace` | Reset |

## Building a SELECT

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

## GROUP BY and aggregates

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
[Aggregated results](RESULTS.md#aggregated-results).

## Schema-aware autocomplete

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
[Filtering results](RESULTS.md#filtering-results)).

## Saved queries

Builders can be saved per connection profile and reopened later. Saved queries
are scoped to the profile, with unique names. A saved query can also be imported
onto a different connection; on import DBFlux verifies that the referenced tables
exist on the target connection before loading.

## Visual UPDATE and DELETE

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
[Dangerous-query confirmation](EDITOR.md#dangerous-query-confirmation)) before it runs.
