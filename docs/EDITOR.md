# Running Queries

Open a new query tab with `Ctrl+n` (`Cmd+n` on macOS), or open a script file with
`Ctrl+o`. The editor's query language (SQL, MongoDB query syntax, Redis commands,
etc.) is determined by the active connection's driver, which also drives syntax
highlighting and the placeholder text.

For SQL connections, the [Visual Query Builder](QUERY_BUILDER.md) composes
queries without writing SQL.

## Saving and closing tabs

A new query tab (`Ctrl+n`) is backed by a real file in your scripts folder, the
same way a script opened with `Ctrl+o` is. Open editors auto-save to that file
on the configured interval, and `Ctrl+s` / **Save File As** go through the same
queue. Autosave and closing never overwrite a file that changed outside DBFlux:
your version stays in the editor and DBFlux reports the refused write. `Ctrl+s`
and **Save File As** are deliberate and write the file even then.

Closing a tab with pending edits saves them first, then closes; if the write
cannot land (for example, the file changed outside DBFlux or is read-only), the
tab stays open with your changes and DBFlux points at `Ctrl+s` / **Save File As**
as the deliberate overwrite. A buffer with no file yet is the exception: closing
it asks first, so you can save it, close it without saving, or cancel. Quitting
DBFlux saves pending edits the same way before it shuts down. If the scripts
folder could not be created at startup, new queries are kept in the session store
instead, and **Save File As** is offered when you close them.

## Executing

- `Ctrl+Enter` (`Cmd+Enter`) — **Run Query**.
- `Ctrl+Shift+Enter` (`Cmd+Shift+Enter`) — **Run Query in New Tab**.

If a non-empty text selection exists, only the selected text runs. With no
selection, the full editor buffer is used.

When execution actually omits rows, the editor reports one warning for the query and the grid marks the affected result set, even if no rows were retained. A result that exactly fills a limit without omitting rows does not trigger the warning. A retained-row cap limits stored rows only; byte and time limits are separate execution controls. This does not imply a default editor row cap.

## Multi-statement scripts

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

## Dangerous-query confirmation

DBFlux detects dangerous operations across languages — SQL `DELETE`/`DROP`/
`TRUNCATE` and `DELETE`/`UPDATE` without a `WHERE`, MongoDB `deleteMany`/`drop`,
Redis `FLUSHALL`/`FLUSHDB`/`KEYS` — and prompts for confirmation before running.
This behavior is governed by settings: dangerous-query confirmation can be turned
off, a `WHERE` clause can be required for `DELETE`/`UPDATE`, and Redis
`FLUSHALL`/`FLUSHDB` can be disabled entirely (in which case those commands are
blocked rather than confirmed).

## Scripts (Lua / Python / Bash)

Lua, Python, and Bash documents execute as scripts rather than database queries.
Their output streams live into the document's output area while running, and the
final output is kept as a text result. See `docs/LUA.md` for the embedded Lua
runtime.

## Saved Queries and History

DBFlux keeps a history of completed queries and lets you save named queries.

- `Alt+h` (in the editor), or the toolbar's History button, opens and closes the
  query history panel beside the editor.
- `Ctrl+s` (`Cmd+s`) — **Save** the current query.
- `Ctrl+Shift+s` (`Cmd+Shift+s`) — **Save File As**.
- `Ctrl+p` (`Cmd+p`, in the editor) — open the saved-queries browser.

The history panel lists recent and saved queries and stays open while you edit.
Clicking an entry or pressing `Enter` loads it into the editor. While the panel
has focus you can navigate with `Ctrl+j`/`Ctrl+k` (or arrow keys), save a recent
query with `Ctrl+s`, and use the local mnemonics `Ctrl+f` (toggle favorite),
`Ctrl+r` (rename), and `Ctrl+d` (delete). `/` or the search button in the panel
header opens the search field, and `Esc` closes the panel.
