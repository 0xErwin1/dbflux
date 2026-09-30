# Key-Value Browser

A key-value database opens as a key browser: the key list on the left, the
value of the selected key on the right, and a command console underneath.

- **Key list.** Keys load a page at a time; **Load more** (`Ctrl+J`) continues
  the scan and the footer reads "Loaded N of total" against the database's key
  count. **Tree** groups keys into folders by the connection's namespace
  delimiter (`:` unless the connection settings set another one) and sorts each
  level; **List** shows every key sorted by name. Folder counts only cover the
  keys loaded so far and read as "≥ N keys" until the whole keyspace has been
  scanned. The TTL and Size columns are filled for the rows on screen, in one
  batch per scroll position; a TTL under a minute shows in red.
- **Pattern and type.** The pattern field takes a glob (`user:*`); plain text
  matches anywhere in the key. The type filter (All, String, Hash, List, Set,
  ZSet, Stream, JSON) runs on the server, so it covers the whole keyspace. A
  filtered search keeps scanning until it has a page of matches; **Stop** ends
  it and **Search whole keyspace** reads to the end.
- **Expiry.** Click the TTL in the value header, or press `t`, to set an
  expiry: **Never**, **In** a duration (`1h`, `24h`, `7d`, `30d`, or any
  `1h30m`-style value) or **At** a local date and time. The absolute expiry
  time is shown before you apply. Editing a value keeps the key's expiry.
- **View as.** String values can be shown as Auto, JSON (pretty-printed with
  line numbers), Text, MsgPack or Hex, after an optional decompression step
  (gzip, zstd, snappy, lz4). Detection only runs on the value you open. A
  value over the preview limit shows its size first, with **Preview first
  64 KB** and **Load anyway**.
- **Collections.** Hashes, lists and sets show a member table with a format
  badge per value. Sorted sets page by rank, highest or lowest first, with a
  bar relative to the top score. Streams page between a start and an end ID,
  newest or oldest first, with one column per field and the consumer groups
  beside them: readers, pending entries, last delivered ID, and **Claim** to
  move pending entries to another reader.
- **Bulk delete.** **Bulk actions → Delete keys matching the pattern** scans the
  whole database for the current pattern and type, lists the first matches with
  the total, and asks you to type the pattern back. **Export keys first** saves
  the keys and their values to a JSON Lines file. Keys are deleted in batches
  with `UNLINK` and the deletion is recorded in the audit log.
- **Console.** `` Ctrl+` `` opens a console that runs commands against the open
  database. Dangerous commands go through the same confirmation as the editor
  (see [Dangerous-query confirmation](EDITOR.md#dangerous-query-confirmation)),
  every command is recorded in the audit log like an editor query, and
  successful commands join the query history. `Up`/`Down` walk through that
  history for the connection, together with the commands the console refused
  or that failed in this session. Document collections offer the same console
  (see [Document Collections](DOCUMENTS.md)).

In the sidebar, each database of a key-value connection shows its key count,
and databases with no keys fold into a single "N empty databases" row.
