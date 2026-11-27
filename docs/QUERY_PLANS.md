# Query plans

**Explain** shows how the database intends to run a statement, so you can see
why it is slow before you run it. The plan appears as a **Plan** tab next to the
result tabs of the editor, as a diagram, a tree or the engine's own text. DBFlux
asks the database for the plan and, for an estimate, never runs the statement.
An analyzed plan, available for some drivers, does run it; see
[Analyzed plans](#analyzed-plans).

## Quick path

1. Put the cursor in a statement, or select the text to explain.
2. Click **Explain** in the editor's side strip, press `Leader e`, or choose
   **Explain query** from the pane actions menu.
3. Read the **Plan** tab: the box with the biggest **self cost** is marked
   **COSTLIEST**, and the thickest line carries the most rows.
4. Where the driver supports it, choose **Analyze** to run a read statement and
   see real times and rows; see [Running an analyzed plan](#running-an-analyzed-plan).

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
| **Tree** | An indented list with Node, Index, Cost, Self cost and Rows columns (an analyzed plan adds Actual rows, Time and Loops) | Scan the numbers in a column |
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
| `Esc` | Close the inspector, close the Analyze card, or cancel a plan that is loading |
| `Alt+H` / `Alt+L` | Previous and next result tab, the Plan tab included |
| `Alt+W` | Close the tab shown |

The view switch, the Estimate | Analyze switch, the zoom controls, **Copy**, **Run
analyze** and the error buttons are entries of the pane actions menu (`m` in the context bar, `Shift+F10` in the editor) while the
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

## Analyzed plans

An **estimate** asks the database how it would run the statement. An **analyzed
plan** runs the statement and reports what happened: the rows each step really
returned, the time each step took, how many times it ran (loops), and the
planning and execution time of the whole statement. Where the numbers differ
from the estimate, the estimate is what misled the planner.

Running the statement has a price, so an analyzed plan is stricter than an
estimate:

- **It only runs read statements.** The shared classifier must mark the
  statement `Read` or `Metadata` (a `SELECT`, or a `WITH` that only reads). An
  `INSERT`, `UPDATE`, `DELETE`, DDL, or a `WITH` that deletes, is refused before
  anything is sent, with: *Analyze is only available for read statements; this
  statement is not a plain read, so only the estimate can run.* The estimate of such a
  statement is still available and never runs it.
- **The server refuses writes too.** The driver runs the statement inside a
  read-only transaction and rolls it back, so a write the classifier cannot see,
  such as a function that inserts, fails instead of changing data.
- **It is one statement**, like an estimate.
- **Side effects a read-only transaction does not stop** (for example advisory
  locks or functions that reach outside the database) still happen. Analyze runs
  the statement for real, so a function with side effects outside the transaction
  (session locks, remote writes through extensions) takes effect exactly as when
  you run the query. The statement
  also takes as long as it takes to run.

| Driver | Analyzed plan | Notes |
|--------|---------------|-------|
| PostgreSQL | `EXPLAIN (ANALYZE, FORMAT JSON)` in `BEGIN READ ONLY` | Planning and execution time; `Rows Removed by Filter` and similar counters stay in the node details |
| MySQL 8.0.18+ | `EXPLAIN ANALYZE` in `START TRANSACTION READ ONLY` | Actuals come from the TREE text; no planning time; MySQL 5.7 has no analyze |
| MariaDB | `ANALYZE FORMAT=JSON` in `START TRANSACTION READ ONLY` | Times are reported over all loops and shown per loop |
| DuckDB | `EXPLAIN (ANALYZE, FORMAT JSON)` in a `READ ONLY` transaction | Operator time is CPU time summed over threads; DuckDB must parse the statement as a `SELECT` |
| SQL Server, SQLite, Turso | Not supported | Answer *Analyzed plans are not supported by this driver* before anything is sent |

A driver advertises analyzed plans with the `supports_explain_analyze` query
capability. The Plan tab shows the Estimate | Analyze switch only on a
connection whose driver has it; on every other connection the switch is not
drawn.

### Running an analyzed plan

1. Explain the statement as usual. The estimate comes first and runs nothing.
2. Choose **Analyze** in the **Estimate | Analyze** switch, next to the view
   switch. This runs nothing either: the tab shows a card, **Analyze runs this
   query on** *connection*, that says the statement really runs to measure real
   time and rows, and that anything it does outside the transaction (locks,
   remote calls) still happens. On a production connection the card also says so.
3. Choose **Run analyze** to send it, or **Cancel** (`Esc`) to go back to the
   estimate. The analyzed request carries the same statement, on the same
   connection, as the estimate: the editor's current connection for **Explain**,
   or the connection the result came from for **Explain this query**.
4. While it runs the tab shows placeholder rows and **Cancel**. The statement
   takes as long as the query takes; Cancel stops it through the same task cancel
   as a query.

When the statement is not a plain read, **Analyze** is dimmed and a band under
the switch says *Analyze is off for this statement. It changes data, so only the
estimate runs.* The estimate stays on screen. If the text holds more than one
statement, Analyze is dimmed with the reason on hover.

The analyzed plan replaces the estimate's numbers in every view, and the tab is
labelled **analyzed**. Choose **Estimate** to see the estimate again; nothing is
asked of the server for that. Both plans stay in the tab until you explain again.
From the keyboard, the pane actions menu has **Estimate**, **Analyze** and, on
the card, **Run analyze**.

| Where | What changes when the plan is analyzed |
|-------|-----------------------------------------|
| Totals | `planning` and `execution` time and the rows returned replace the total cost |
| Diagram | Each box adds its time, loops and actual rows beside the estimate. **SLOWEST** (most self time) replaces **COSTLIEST**, the bar shows self time as a share of execution, and a line's thickness and label follow actual rows times loops |
| Gap marker | The box whose actual rows are furthest from the estimate gets a badge with the factor (for example `15.5×`) only when it is 10 times or more, and the warnings band says the same. Hover the badge for the numbers |
| Tree | Node, Index, Cost, Est. rows, Actual rows, Time, Loops and Self time; a column with no data is left out |
| Inspector | **Estimate** and **Actual** sections, with **Versus estimate** (for example `15.5× more rows`) |
| Footer | **Analyzed · query was run** instead of **query not run** |

### Reading analyzed numbers

| Number | Meaning |
|--------|---------|
| **Actual rows** | Rows the step returned, per loop |
| **Loops** | How many times the step ran. A step that never ran has zero loops |
| **Actual time** | Milliseconds across all loops, until the last row |
| **Self time** | The step's time over all its loops minus its children's, never below zero. It points at where the time went |
| **Estimate gap** | Actual rows against estimated rows per loop, in either direction. The largest gap is flagged only when it is 10 times or more, and the deepest step wins a tie |

## Limits

- An estimate is a guess made without running the statement, so it has no real
  timings or row counts. Use an analyzed plan, where the driver has one, for those.
- A write statement is planned without running, like any other, and is never
  analyzed.
- A plan covers one statement at a time.
- A very large raw plan (an XML plan of a big query) is long; it scrolls inside
  the tab.
