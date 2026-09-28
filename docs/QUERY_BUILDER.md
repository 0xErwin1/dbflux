# Visual Query Builder

For SQL connections you can compose queries without writing SQL. From a table's
data grid toolbar, click **Builder** to open a right-rail panel. Document
collections have their own builder, described under
[Document collections](#document-collections).

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
- **Sort and limit** — `ORDER BY` entries and the paging bounds. Each sort row
  is a column picked from a dropdown (source columns, then the columns of joined
  tables) and an ASC/DESC switch; the first row also holds the limit. The last
  line adds another sort column and holds the offset. Drivers that cannot sort
  show only the limit and offset.

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
- The **Sort** dropdowns offer only group columns and aggregate aliases.

How grouped results behave in the data grid is described under
[Aggregated results](RESULTS.md#aggregated-results).

## Schema-aware autocomplete

The builder's single-line inputs (filter, projected columns, the join
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

## Keyboard

`Ctrl+l` from the table's grid moves the keyboard into the builder. `j` and
`k` move a cursor over its rows, `h` and `l` between the fields of a row, and
`Enter` works the field: it types in a text field, opens a dropdown or presses
a button. `a` adds an entry, `Shift+a` a filter group, `x` removes the row and
`Space` flips its switch. `Alt+l` and `Alt+h` switch the mode, `m` opens a menu
with Run, Save, Reset and the rest, and `Escape` or `Ctrl+h` go back to the
grid. The full list is in [Query builders](KEYBOARD.md#query-builders).

## Document collections

Collections on drivers that offer it (MongoDB) have a visual builder for find
queries and simple aggregations. Click **Builder** in the collection header to
open it in the right rail; click it again, or the rail's close button, to hide
it. The draft stays while the tab is open. On a document driver without a
builder, such as DynamoDB, the button is disabled and its tooltip says why; the
query slots still work.

The rail has a **Find** / **Aggregate** mode switch, the cards described below,
and a preview of the query in the driver's own syntax, pinned above the footer.
The footer runs the query (**Find** in Find mode, **Run pipeline** in Aggregate
mode) or opens the preview in a query editor (**Open in editor**).

### Find mode

| Card | What it builds |
|------|----------------|
| **Filter** | Conditions combined as *all of* (`$and`) or *any of* (`$or`), with nested groups. |
| **Project** | Fields to include or to exclude. `_id` is always returned, so result rows stay editable. |
| **Sort, limit and skip** | Sort keys, each ascending or descending, then a limit and a skip. |

A condition is a field, an operator and a value. The operators offered follow
the field's type in the schema sample, from `$eq`, `$ne`, `$gt`, `$gte`, `$lt`,
`$lte`, `$in`, `$nin`, `$regex`, `$exists`, `$elemMatch`, `$size` and `$all`. A
field sampled with more than one type is flagged, and its conditions offer the
operators of each type. The value input follows the operator:

| Operator or type | Value input |
|------------------|-------------|
| `$in`, `$nin`, `$all` | A list of values, one chip each (type a value, press `Enter`). |
| `$exists`, and `$eq` / `$ne` on a boolean | A true / false switch. |
| `$regex` | A pattern, `/pattern/flags` or a bare pattern. |
| `$size` | A whole number of elements. |
| `$elemMatch` | Conditions that one array element must meet. |
| Date field | `YYYY-MM-DD`, `YYYY-MM-DD HH:MM` (UTC) or an RFC 3339 timestamp. |
| ObjectId field | 24 hexadecimal digits. |

Fields come from a picker fed by the Schema view's sample: nested paths, a type
tag for each and how often the field appears. A path the sample never saw can
be typed and used with `Enter`; it is marked as unsampled.

**Find** runs the query through the query bar, like the slots do: results are
ordinary documents, editable, counted and kept in the query history.

### Sync with the query bar

While the rail is open, the builder and the `filter`, `project`, `sort` and
`limit` slots stay in sync both ways: a builder edit rewrites the slots it
changes, and a slot edit reloads the builder, which discards a condition you
have not finished. Slots changed while the rail was closed, or while the
builder was in Aggregate mode, are read again when the rail reopens or the
builder returns to Find: a part the builder did not change in the meantime
takes the slot's query. Running a query from the history also reloads the
builder and clears its skip.

The skip has no slot; it applies while the rail is open, and paging counts from
it: going back a page stops at the skip, and a new page size starts over there.

When a slot holds a clause the builder cannot show, such as `$expr`, the
builder shows the parts it understands, keeps the rest read-only and shows a
card with two choices. Edits in the builder that would change that slot wait
until you choose, and **Find** stays disabled while such an edit is waiting.
With no edit waiting, **Find** runs the slots as written.

| Choice | Effect |
|--------|--------|
| **Keep the text** | Discards the waiting builder edits and keeps the slot as written. |
| **Rewrite from builder** | Replaces the slots with the builder's query, dropping the clauses it could not show. |

### Aggregate mode

Adding a group stage in the **Group** card switches to **Aggregate**, and
removing it returns to **Find**. Aggregate mode needs a driver that runs
aggregation pipelines.

- **Group by** takes zero or more fields; with none, all documents form one
  group.
- **Accumulators** are `$count`, `$sum` and `$avg`, each with an output name.
  `$sum` and `$avg` take a number field.
- The filter becomes a `$match` stage, shown as a summary with **Edit**.
- **Project** is not used: the output fields are the group key and the
  accumulators.
- Sort keys can only be group keys or accumulators.

**Run pipeline** writes the pipeline into the collection's Aggregate view and
runs it there. Grouped rows are computed, so they have no `_id` to edit: the
results are read-only and carry a banner that says so. While the builder is in
Aggregate mode, the query bar and the Aggregate view show a summary of the
pipeline instead of the slots and the pipeline editor. While the Aggregate view
is running a pipeline or waiting for a confirmation, **Run pipeline** leaves it
alone and says that nothing ran.

### Saved document queries

Name the query in the rail header and click **Save query**. Saved queries
belong to the collection: its connection profile, database and collection.
Saving under a name that already exists replaces that query. **Saved queries**
lists them; opening one loads it in the mode it was saved in. Opening a saved
find replaces all four slots, including clauses the builder cannot show.

### Limitations

- A text value of 24 hexadecimal digits runs as an ObjectId, because the driver
  converts such strings. The builder asks for it as an ObjectId, so a text field
  holding such a string cannot be searched as text.
- Decimal values compare as doubles, so they do not match `Decimal128` fields.
- In the slots, `{"$date": "..."}` accepts only an RFC 3339 timestamp, such as
  `2024-03-09T14:30:05Z`.
- The builder has no write stages (`$out`, `$merge`) and does not build updates
  or deletes.
- Deleting a saved query does not ask for confirmation.
