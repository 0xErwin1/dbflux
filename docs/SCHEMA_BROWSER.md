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

## External scripts folders

The Scripts view can list scripts that live outside DBFlux's own scripts
folder, such as a repository of SQL files shared across projects, without
copying them. Choose **Add external folder** from the `+` menu of the Scripts
view, or run **Add external scripts folder…** from the command palette, and
pick the folder.

- The folder appears below your own scripts, with its name as the label. Only
  files the editor opens (`.sql`, `.js`, `.redis`, `.lua`, `.py`, `.sh` and the
  other script types) are listed. Subfolders that hold only other files are
  hidden; empty ones are shown, so a folder you just created stays visible.
  Symbolic links that lead out of the folder are not listed, and nothing is
  written through them.
- Scripts open and save in place, so the files stay in sync with the folder.
  Creating, renaming and deleting files and subfolders inside it works as in
  your own scripts folder. Moving a file between two different folders is
  refused.
- DBFlux does not watch the folder. Press `r` on the folder, or choose
  **Refresh** from its menu, to pick up files added or changed outside DBFlux.
  The folders are also scanned in the background each time DBFlux starts.
- If the folder is moved, deleted, unmounted or cannot be read, it stays in the
  list marked **unavailable** until it comes back or you remove it. Its menu
  says why it could not be read.
- **Remove from sidebar** (`x`) only forgets the folder. Its files are never
  touched, and the folder itself cannot be renamed, moved or deleted from
  DBFlux.
- External folders are not shared with AI clients: the MCP script tools only
  see DBFlux's own scripts folder.

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
