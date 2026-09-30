# Document Collections

A collection on a document database opens as a tree of its documents, one
collapsible node per document. **Table** and **JSON** show the same page; switch
with the Tree / Table / JSON control or press `t`. The view you pick stays for
the tab across pages and refreshes. The table puts `_id` first, then the fields
in the order the page returns them.

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
  flagged, and clicking a value adds it to the filter. The Schema view shows
  only the sample controls and the field table, without the query bar, the
  documents footer and the document count in the header.
- Drivers that run aggregation pipelines (MongoDB) add an **Aggregate** view
  after Schema. Write the pipeline as a JSON array of stages, relaxed keys
  allowed (`[{ $match: { status: 'paid' } }, { $group: { _id: '$region' } }]`),
  and run it with **Run** or `Ctrl+Enter`. A pipeline that does not parse, or
  a stage that is not a document naming one `$` operator, is reported under the
  editor and nothing runs. The result documents show in their own Tree, Table
  and JSON views, opening in the Tree, apart from the Documents page, and are read-only: nothing in
  them can be edited, deleted or committed. A run shows at most 1,000
  documents, and the footer says when the result was cut. A pipeline with an
  `$out` or `$merge` stage writes to a collection, so it asks for the same
  confirmation as any other dangerous query before it runs. The history button
  brings back an earlier pipeline of the same tab.
- Drivers that offer it (MongoDB) add a **Builder** button to the collection
  header. It opens a visual query builder in the right rail that stays in sync
  with the query bar slots, can group documents with `$count`, `$sum` and
  `$avg`, and saves queries per collection. While the builder is in Aggregate
  mode, the query bar and the Aggregate view show a summary of its pipeline
  instead of the slots and the pipeline editor, and **Run pipeline** runs it in
  the Aggregate view. On other document drivers the button is disabled. See
  [Document collections](QUERY_BUILDER.md#document-collections) in the query
  builder guide.
- The shortcut or row action that opens the row inspector on a table opens the
  **Document** panel on a collection: the document's size, then its fields as a
  tree of `key : value` rows with the value colored by type and a short type
  (`oid`, `obj`, `str`, `arr`, `date`, `dec`, `bool`, ...) at the right.
  Objects and arrays expand and collapse with a click; top-level fields start
  expanded. The expand button opens the document in the JSON editor. The panel
  follows the selected row, and a field with a staged, uncommitted edit shows
  its new value, highlighted like the edited cell.
- With MongoDB, table edits are staged: an edited cell shows its old and new
  value, the cell menu offers **Revert change** and **Unset field**, and
  **Commit** (`Ctrl+S`) writes each document as `$set` / `$unset` on the changed
  paths only. The JSON view replaces whole documents; tree edits are written at
  once. Before writing, DBFlux reads the document again: if it changed on the
  server after the page loaded, a card says whether your change can apply on
  top, shows the exact update, and offers **Reload document** or **Apply my
  change**.
- Drivers that offer a native console (MongoDB) dock it under the collection:
  `` Ctrl+` `` shows or hides it. It runs one shell command at a time, such as
  `db.orders.find({"status": "paid"})`, against the collection's database and
  prints documents as one JSON object per line. A command that writes
  refreshes the documents on screen. See [Console](CONSOLE.md).
