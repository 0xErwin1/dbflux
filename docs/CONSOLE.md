# Console

The console runs one native command at a time against a database and prints
the result as text, the way a command-line client does. It sits next to the
query editor rather than replacing it: the editor is the place for longer
queries, result grids, charts and saved files; the console is for a quick
command without leaving the table, collection or keys you are looking at.

Drivers opt in to it. PostgreSQL, MySQL, MariaDB, SQL Server, SQLite,
Redshift, ClickHouse, Turso, MongoDB and Redis offer it; other drivers show no
console.

## Where it opens

| Where | How | Runs against |
|---|---|---|
| Under a table | `` Ctrl+` ``, or click the **Command** bar at the bottom | the table's database |
| Under a document collection | `` Ctrl+` ``, or click the **Command** bar | the collection's database |
| Under the key-value browser | `` Ctrl+` ``, or click the **Command** bar | the open database |
| In its own tab | **Open console** in a database's menu in the sidebar | that database |

A docked console collapses to its bar; `` Ctrl+` `` opens it with the keyboard
in its input and closes it again, also from the input. A console tab stays open
and `` Ctrl+` `` moves the keyboard back to its input. Opening the console of a
database that already has a tab switches to that tab.

## Running commands

Type a command and press `Enter`. The prompt names the database (`shop>`,
`0>`). Completion follows the connection's language, as in the editor: table
and column names for SQL, collection methods for MongoDB, commands for Redis.
While the completion list is open, `Up` and `Down` move in it; otherwise they
walk the history.

Results print as:

- text as the server returned it;
- documents as one JSON object per line;
- a single column as a numbered list;
- several columns as an aligned table with a header;
- binary values as their size.

Every result set of a batch is printed, each after a separator. At most 200
lines of a result are shown, with a count of the rest; use the editor for large
results.

A command that writes (not a read) refreshes the table or collection it is
docked under. In the key-value browser, a successful command reloads the open
key.

## Protections

The console applies the same checks as the editor before anything runs:

- The driver's language service validates the command, and a command it
  rejects is not sent.
- Dangerous commands (a `DELETE` or `UPDATE` without `WHERE`, `DROP`,
  `TRUNCATE`, `FLUSHDB`, `deleteMany`, ...) ask for confirmation according to
  the settings described in
  [Dangerous-query confirmation](EDITOR.md#dangerous-query-confirmation). A
  command the driver does not flag but that is rated destructive asks as well.
  **Run anyway** or `Enter` in the empty input runs it; **Cancel** or `Escape`
  drops it.
- A confirmed command carries the permission it was confirmed for, so a
  MongoDB script typed into the console follows the same ceiling rules as one
  run from the editor.
- Every command is recorded in the audit log like an editor query, and every
  confirmation is recorded too.

On drivers that can enforce it, the **Editor row limit** from
[Settings](SETTINGS.md) caps the rows a command returns, and a warning says
when rows were left out. Drivers that cannot enforce a row limit (MongoDB,
Redis, Turso, ClickHouse, Redshift) run console commands without one, where
the editor refuses the query instead; add a limit to the command itself
(`LIMIT`, `.limit()`) when the result may be large.

Where the driver offers an isolated editor session (Turso), the console uses
one of its own, so a transaction opened by one command stays open for the
next. On the other drivers each command runs on the connection like an editor
query.

## History

Commands that succeed are added to the query history, next to the editor's.
`Up` and `Down` walk the connection's history together with the commands the
console refused, cancelled, or that failed in this session. Multi-line entries
from the editor are left out, because the console takes one line.
