# Browsing the Schema

The sidebar has three views, chosen from the activity rail on its left:

- **Connections** — the schema tree (databases, schemas, tables/collections,
  columns, indexes, and — where the driver supports it — a Routines folder).
- **Scripts** — file and folder management for saved query files, script hooks,
  and other user files.
- **Dashboards** — every saved dashboard, grouped by the connection it belongs
  to, whether or not that connection is open. Dashboards without a connection
  are listed under **No connection**. Double-click a dashboard, or select it
  and press `Enter`, to open it; right-click it to open, rename, duplicate or
  delete it. The `+` button creates a dashboard.

Cycle through the views with `q` or `e`. Choosing the view already on screen
from the rail collapses the sidebar.

## Navigating the tree

- `j`/`k` (or `Down`/`Up`) — move the selection.
- `h` collapses, `l` expands the current node. `Space` toggles expand/collapse.
- `g` jumps to the first item, `Shift+g` to the last; `Home`/`End` do the same.
- `Ctrl+d`/`Ctrl+u` (or `PageDown`/`PageUp`) — page through long lists.
- `/` focuses the sidebar search/filter.
- `Enter` opens the selected item (for example, a table opens a data grid).
- `r` refreshes the schema; `d` disconnects the active connection.
- `m` opens the context menu for the selected item.

## Lazy loading

Schema is loaded lazily. On connect, DBFlux fetches shallow metadata (names).
Detailed metadata — columns, indexes, and similar — is fetched on demand when you
expand a node. This keeps the initial connection fast on large databases.

## Collapsed sidebar preview

When the sidebar is collapsed, hovering over it reveals it after 250 ms; entering
with the keyboard (FocusSidebar, focus cycling, directional navigation, or the
command palette) reveals it immediately. The preview closes once both pointer
and focus have left, unless a menu, child picker, tracked drag-and-drop target,
or resize is active. Outside a preview, ToggleSidebar (Ctrl+B) changes the
explicit collapsed/expanded choice. During a preview, Ctrl+B or the visible
left-chevron only closes the preview. The migration wizard's source and target
pickers share the lazy hierarchy but keep their own selections.

## Routines / stored procedures

For drivers that advertise routine support (PostgreSQL is the first
implementation), the schema tree includes a **Routines** folder containing
functions, procedures, aggregates, and window routines. Opening a routine opens a
read-only code document showing its definition. The document is non-editable but
you can still select and copy its text; execution and mutation controls are
hidden.

## Schema diagram

Relational connections whose driver reports foreign-key support (for example
PostgreSQL, MySQL/MariaDB, SQLite, and SQL Server) can draw tables and their
foreign keys as a diagram. Open it from the sidebar context menu:

- **View Schema Diagram** on a loaded database draws every table in it, up to
  100. A larger database shows the notice "Showing first 100 tables — the
  schema has more."
- **View Relationships** on a table draws that table, the tables it references,
  and the tables that reference it.

The diagram opens in its own tab, and opening the same diagram again switches to
that tab. Loading runs as a background task ("Schema diagram: _database_") that
you can cancel from the Tasks panel. Each table lists its columns with `PK`,
`FK`, and `NN` (not null) badges, and lines connect each foreign key to the
table it references.

| Toolbar control | What it does |
|---|---|
| `+` / `-` | Zoom in / out, between 25% and 400%. The current zoom is shown beside them. |
| **Reset** | Returns to 100% zoom and the starting position. |
| **Arrange** | Discards the positions of tables you moved and recomputes the layout. |
| **Fit** | Zooms and pans so every table is visible. |
| Layout dropdown | **Left to right** (default) places tables that hold foreign keys on the left and the tables they reference on the right. **Snowflake** puts one table in the center and its direct neighbors in a circle around it: the chosen table for **View Relationships**, the most connected table for a database diagram. **Compact** packs tables into a tight grid sorted by name. Changing the layout also discards moved tables and fits the diagram into view. |
| **Export** | **Copy as DBML** or **Copy as SQL** copies the tables shown in the diagram to the clipboard. The SQL is `CREATE TABLE` statements plus `ALTER TABLE ... ADD CONSTRAINT` for the foreign keys. |
| _N_ tables · _M_ relations | How many tables and foreign keys the diagram shows. |
| **Types** / **Indexes** | Show column types (on by default) / an index list under each table (off by default). |

Drag empty space to pan, drag a table to move it (it snaps to the grid), and use
the mouse wheel to zoom around the pointer. A database diagram opens fitted into
view. Click a table to select it and open its details in the panel on the right:
the qualified table name, a summary line (columns, indexes, foreign keys, and how
many foreign keys reference it), then INDEXES, FOREIGN KEYS, and REFERENCED BY.
Each section appears only when it has entries. While the panel is open, selecting
another table with the keyboard moves the panel to it. Right-click
opens a context menu with **Zoom in**, **Zoom out**, **Layout**, and **Copy as**.
Right-clicking a table also selects it, and while a table is selected the menu
adds **Inspect schema**, which opens the same panel. Keyboard shortcuts are listed in
the [Keyboard Reference](KEYBOARD.md#schema-diagram).
