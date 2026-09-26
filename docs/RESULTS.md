# Working with Results

Results render in result tabs inside the document. The view mode is chosen
automatically from the database category:

- **Table view** for relational databases.
- **Table view** for document collections (for example MongoDB, DynamoDB), with
  **Tree** and **JSON** views of the same page (see
  [Document collections](DOCUMENTS.md)).
- **Key-value view** for Redis (see [Key-Value Browser](KEY_VALUE.md)).

Event-stream-style containers open as event streams when the driver declares that
presentation.

## Navigating the data grid

When the results panel has focus:

- `j`/`k` (or `Down`/`Up`) — move between rows.
- `h`/`l` (or `Left`/`Right`) — move between columns.
- `g`/`Shift+g` (or `Home`/`End`) — first / last row.
- `Ctrl+d`/`Ctrl+u` (or `PageDown`/`PageUp`) — page through rows.
- `[` / `]` — previous / next page of results (pagination).
- `f` focuses the toolbar; `/` focuses the search/filter.
- `z` toggles collapsing the panel.
- `m` (or `Shift+F10`) opens the row/cell context menu.

## Record view

Press `i`, or use the Record toggle in the result status bar, to show
the active row as a Name / Value list that fills the result area. The header
names the row's position in the result. Fields are edited exactly like grid
cells, so unsaved changes, Save Row and revert work the same in both layouts;
`Up`/`Down` move between fields and `Left`/`Right` move between rows. Press
`i` again to return to the grid.

## Column header menu

Right-click a column header for a menu scoped to that column: order ascending
or descending, clear the ordering, and every filter operator, in one flat
list. A left click on the header still cycles the sort.

## Value panel

Right-click a cell and choose **View Value**, or press `v`, to open the cell
in the inspector rail on the right. The panel shows the value as JSON, XML or
plain text — detected from the content, and only when it really parses — with
pretty-print, compact and word wrap. You can edit there: **Save** commits the
row directly, **Revert** discards the edit. The panel follows the selected cell
as you move through the grid, except while it holds an unsaved change.

## Row inspector

Press `Ctrl+Space`, or right-click a row and choose **Inspect Row**, to open
the selected row in the inspector rail on the right. It lists every column of
the row with its value, marks primary and foreign key columns, and under
**References** names the table each single-column foreign key points at, with
the referenced row once it resolves. The inspector follows the selected row as
you move through the grid; the pin button in its header keeps it on the current
row instead. **Edit**, **Duplicate** and **Delete** at its foot act on the
inspected row when the result is editable. Press `Ctrl+Space` again, or the
close button, to dismiss it.

## Filtering results

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
builder — see [Schema-aware autocomplete](QUERY_BUILDER.md#schema-aware-autocomplete)).

## Editing and CRUD

In the data grid:

- `o` — add a row.
- `x` — delete the selected row.
- `r` — rename / edit (context-dependent).
- `y` — copy the selected row.
- `Ctrl+c` (`Cmd+c`) — copy the selected cell(s) to the clipboard.

### When results are editable

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

### Aggregated results

When a result comes from a grouped (`GROUP BY`) query, rows show the aggregated
output and editing is disabled — add-row, delete-row, edit-cell, and inspect-row
are unavailable, with explanatory tooltips. Pagination counts the grouped rows
(not the underlying rows), so the page total is accurate. Aggregate columns keep
the correct column kind, so charting still works.

## Copy as Query

The result context menu includes **Copy as Query**, which generates a
driver-specific mutation statement (or envelope, for non-SQL drivers) from the
selected row using the driver's own query generator.

## Exporting

Press `Ctrl+e` (`Cmd+e`) in the results panel, or run **Export Results** from the
command palette. The available formats depend on the result shape and include:

- **CSV**
- **JSON (pretty)** and **JSON (compact)**
- **Text**
- **Binary** (for binary-shaped results)
