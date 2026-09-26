# Document Collections

A collection on a document database opens as a table of its documents: `_id`
first, then the fields in the order the page returns them. **Tree** and **JSON**
show the same page; switch with the Tree / Table / JSON control or press `t`.

- A nested object shows its field count and an array its length. `e` on an
  object column expands it in place into a column group; `Enter` on an object or
  array steps into it and lists its contents as rows, with a breadcrumb;
  `Backspace` steps out.
- The footer counts documents. When the driver can only estimate how many
  documents match, the count is labelled as an estimate.
- Drivers that support it (MongoDB) add a query bar with four slots — `filter`,
  `project`, `sort` and `limit` — each taking relaxed JSON with field-path
  completion from a sample of the collection. `Ctrl+Enter` or **Find** runs the
  query; the history button runs an earlier one again.
- The **Schema** view, next to Documents, samples the collection and lists each
  field path with its type distribution, presence and a summary of its values.
  The sample size can be changed, fields that hold more than one type are
  flagged, and clicking a value adds it to the filter.
- With MongoDB, table edits are staged: an edited cell shows its old and new
  value, the cell menu offers **Revert change** and **Unset field**, and
  **Commit** (`Ctrl+S`) writes each document as `$set` / `$unset` on the changed
  paths only. The JSON view replaces whole documents; tree edits are written at
  once. Before writing, DBFlux reads the document again: if it changed on the
  server after the page loaded, a card says whether your change can apply on
  top, shows the exact update, and offers **Reload document** or **Apply my
  change**.
