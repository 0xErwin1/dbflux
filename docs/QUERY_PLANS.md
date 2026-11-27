# Query plans

**Explain** shows how the database intends to run a statement, so you can see
why it is slow before you run it. The plan appears as a **Plan** tab next to the
result tabs of the editor, as a diagram, a tree or the engine's own text. DBFlux
asks the database for the plan and never runs the statement.

## Quick path

1. Put the cursor in a statement, or select the text to explain.
2. Click **Explain** in the editor's side strip, press `Leader e`, or choose
   **Explain query** from the pane actions menu.
3. Read the **Plan** tab: the box with the biggest **self cost** is marked
   **COSTLIEST**, and the thickest line carries the most rows.

To plan the query behind a result, right-click a cell (or press `m`) and choose
**Explain this query**: it explains the query that produced that result tab, not
the text now in the editor, and is offered only when the connection's driver has
query plans. On a driver that keeps one session for every database (such as
MySQL or SQL Server), a result is not planned once the session has moved to
another database: the Plan tab says so, and you switch back and explain again.
The same goes for **Explain** in the editor when the session is no longer on
the database the editor shows.

Explaining again replaces the Plan tab. Close it with the button on the tab, or
with `Alt+W` while it is shown.

## What gets explained

| You have | DBFlux explains |
|----------|-----------------|
| A selection | The selected text |
| No selection, several statements | The statement the cursor is in (or the last one that starts above it) |
| No selection, one statement | The whole buffer |

A plan covers exactly one statement. If the text holds more than one (for
example you selected a whole script), the tab shows the database driver's message
and sends nothing as one string. Put the cursor in a statement, or select one.

The line above the plan says what it covers, for example
`Statement L7–16 · total cost 9,639.43 · rows 50`. The totals are left out when the
engine does not report them.

## The three views

The switch under the result tabs chooses the view. `Shift+T` cycles through them
while the keyboard is in the results, and the pane actions menu lists each one.

| View | Shows | Use it to |
|------|-------|-----------|
| **Diagram** (default) | One box per step, read from the left, ending at the final result | See the shape of the plan and where the rows flow |
| **Tree** | An indented list with Node, Index, Cost, Self cost and Rows columns | Scan the numbers in a column |
| **Raw** | The plan exactly as the engine returned it, with **Copy** | Check a detail the other views leave out |

A column the engine has no data for is left out of the Tree, so a plan without
costs is a short list rather than a table of dashes.

### The diagram

- **Boxes** show the operation, the table and index it reads, one key condition
  (`Filter`, `Index Cond`, `Join cond` or `Sort Key`), the cost, the rows and the
  node's **self cost** as a percentage with a bar. Anything the engine does not
  report is left out of the box instead of being guessed.
- **Lines** run from a step to the step that uses its rows. The label is the
  estimated rows, and a thicker line means more rows (on a log scale, so one
  huge line does not flatten the rest). When the engine reports no rows, every
  line has the same thickness.
- **Zoom and pan**: drag to pan, scroll to zoom, or use the controls in the switch
  row. **Fit** frames the whole plan but never shrinks the text below a readable
  size; a big plan is shown at that size and you pan for the rest.
- Hover a name, a number, a line or a switch for a short explanation. Tooltips
  appear only when you hover, never because the tab opened or a node has focus.

## Reading the numbers

| Number | Meaning |
|--------|---------|
| **Cost** | The planner's estimate for the step, children included, in the engine's own units. Compare costs only inside one plan, never between plans or engines |
| **Startup cost** | The estimate before the step can return its first row |
| **Self cost** | The step's cost minus its children's, as a share of the plan total. It points at where the time goes, which a total cannot |
| **Rows** | The rows the planner expects this step to pass on. An estimate, not a count |
| **COSTLIEST** | The step with the highest self cost |

These are estimates made without running the statement. They are right often
enough to find the expensive step and wrong often enough that you should confirm
a fix by running the query.

## The node inspector

Select a node (click it, or move with the arrow keys) and the right rail shows
it. It starts with the plain-language description of the operation, then
**Estimate** (startup, total and self cost, rows), **Reads** (the relation and
the index) and **Details** (the engine's other attributes). The description is
the same text the tooltip shows, so it is there for keyboard use too.

## Keyboard

| Keys | Action |
|------|--------|
| `Ctrl+J` from the editor, `Ctrl+K` from the results | Move between the editor and the results |
| Arrow keys, `h` `j` `k` `l` | Select the nearest node (Diagram), or the previous and next row (Tree). In the Tree, left folds a branch and right opens it |
| `g` / `Shift+G` | First and last node |
| `Enter` | Show the selected node in the inspector |
| `Shift+T` | Next view |
| `Esc` | Close the inspector, or cancel a plan that is loading |
| `Alt+H` / `Alt+L` | Previous and next result tab, the Plan tab included |
| `Alt+W` | Close the tab shown |

The view switch, the zoom controls, **Copy** and the error buttons are entries of
the pane actions menu (`m` in the context bar, `Shift+F10` in the editor) while the
Plan tab is shown.

## When there is no plan

| You see | Why | What to do |
|---------|-----|------------|
| Explain is disabled, with a tooltip | The connection's driver says it has no query plans | Nothing: the driver cannot produce one |
| **Query plans are not supported by this driver** | The driver accepted the request but has no plan for it | Use a connection whose driver provides plans |
| **More than one statement** | The text held several statements | Put the cursor in one, or select one |
| **Plan failed**, with the server's message | The server refused, for example a table that does not exist | Fix the statement. **Retry**, **Copy error** and **View in Audit** are on the banner |
| **Plan cancelled** | You cancelled while it loaded | **Retry** |

While the plan loads, the tab shows placeholder rows and **Cancel** (`Esc`).
The editor stays responsive.

## Which drivers provide plans

| Driver | Plan | Has costs and rows |
|--------|------|--------------------|
| PostgreSQL | `EXPLAIN (FORMAT JSON)` | Yes |
| MySQL, MariaDB | `EXPLAIN FORMAT=JSON` | Yes |
| SQL Server | The estimated plan (`SHOWPLAN_XML`) | Yes |
| DuckDB | `EXPLAIN (FORMAT JSON)` | Rows only |
| SQLite, Turso | `EXPLAIN QUERY PLAN` | No: the diagram shows the steps, the tables, the indexes and the conditions |

Other drivers answer that they have no plans. Each driver also supplies the
plain-language text for its own operations, so those explanations are in the
engine's vocabulary and are not translated.

## Limits

- The plan is an estimate. Nothing is executed, so there are no real timings or
  row counts. An analyzed plan, which does run the statement, is not offered yet.
- A write statement is planned without running, like any other.
- A plan covers one statement at a time.
- A very large raw plan (an XML plan of a big query) is long; it scrolls inside
  the tab.
