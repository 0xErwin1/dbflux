# CSV and TSV Files

A `.csv` or `.tsv` file, local or in object storage, opens in its own tab as a
table with one row per record. A second view shows the same records as text.
You can edit in either view, and a save writes only the records you changed:
every other record keeps its bytes as they are in the file.

## Opening a file

- **Local file.** Open it with `Ctrl+o` (`Cmd+o` on macOS); the dialog has a
  **CSV / TSV** filter. Recent files, the command palette, the Scripts view of
  the sidebar and `dbflux <file>` from a terminal open it the same way. The
  extension decides, in any letter case: every other file opens in the
  [query editor](EDITOR.md).
- **Object storage.** In the object browser, choose **Open in editor** in the
  object's menu (`m`) or in its preview header. It works whatever the object's
  size, because the file is read in pages. A single click still shows the
  plain-text preview.

DBFlux works out the dialect from the start of the file: the delimiter (comma,
tab, semicolon or pipe), the quote, whether the first record is a header row,
and the text encoding. It then shows the first 500 records.

## The toolbar

| Control | What it does |
|---------|--------------|
| **Delimiter**, **Quote**, **Encoding** | Show the detected value, marked "(detected)", and let you choose another one. |
| **Header row** | Reads the first record as column names, or as data. |
| **Reset to detected** | Drops every choice you made. |
| **Insert row above**, **Add column**, **Discard changes**, **Save** | Edit the file. Shown only for a file that can be saved. |
| **Reload from file** | Reads the file again from disk or from the object store. |

A dialect change reads the file again and never rewrites it. It is refused while
the file has unsaved changes: save or discard them first.

The footer shows how many records are loaded. **Load more** (`]`) reads the next
500, and disappears once the whole file is loaded.

## Table and text views

`t` switches between the table and the text. The text has two modes, switched
with `Shift+T`:

- **Raw** shows the loaded records as a save would write them: untouched records
  exactly as they are in the file, edited ones as they will be written. You can
  edit it.
- **Aligned** pads the columns so they line up, for reading only. A line break
  inside a value shows as `↵`, a tab as `⇥`, and a value longer than 40
  characters is cut.

While you type in the text, its keys are the editor's: `Escape` gives the
keyboard back to the tab, so `t`, `Shift+T` and the other tab keys work again,
and `Enter` returns to the text. Switching views keeps every pending change.

An edit of the raw text is applied to the table when you switch views, save or
load more. Only field values are taken from the text, so a line whose values did
not change keeps its line break and quoting from the file. Text that cannot be
applied, such as a quote that is never closed, stays in the editor with the
reason below it, and the action waits until you fix it.

## Editing

The table takes the [data table keys](KEYBOARD.md#data-table): `Enter` or `F2`
edits a cell, `a a` adds a row, `Shift+a Shift+a` duplicates one, `d d` deletes
one, and `u` or `Ctrl+z` undoes. A cell with more than 100 bytes of text, or
with a line break, opens in a larger editor. A file has no NULL, so **Set NULL**
(`Ctrl+n`) leaves the field empty.

- **Insert row above** adds an empty row above the cursor.
- **Add column** adds a column after the last one and asks for its name. It needs
  every record of the file loaded: on a partly loaded file it offers to load the
  rest, which reads every remaining record into memory and can be cancelled from
  the footer. In a file without a header row the name is not written to the file.
- **Rename column** renames the column the cursor is in. Right-click its header
  for the same prompt. It needs a header row.
- **Discard changes** drops every pending change.

`m` opens the pane actions menu: the dialect controls, **Insert row above**,
**Add column**, **Rename column**, **Discard changes**, **Reload from file**, and
**Cancel loading** while the rest of the file loads.

## Saving

`Ctrl+s` (`Cmd+s` on macOS) saves from the table and from the text, and
`Ctrl+Enter` saves from the table. A save:

- copies every record you did not edit byte for byte, with its quoting and line
  ending, so a save without changes leaves the file identical. A column you
  added is the exception: its value goes at the end of every record, before the
  line ending;
- writes edited and added records in the file's dialect and encoding, quoting a
  value only when it needs it. An edited record keeps its line ending, and an
  added one takes the line ending of the record next to it;
- writes the edited file to a temporary file first and only then replaces the
  original, so a failed save never leaves half a file;
- is refused when the file changed elsewhere since it was opened. Your changes
  are kept: discard them and choose **Reload from file** to see the new content.

A saved local file is a new file under the same name. It keeps the old
permissions, but its owner becomes you, access control lists and extended
attributes are not kept, and other hard links to it keep the old content. Saves
to object storage are recorded in the [audit log](AUDIT.md).

After a save, and with **Reload from file** (`F5`), DBFlux reads the file again,
as many pages as were loaded, up to 20. A reload is refused while the file has
unsaved changes.

### Quitting with unsaved changes

When you quit DBFlux, a local file with unsaved changes is saved as it quits,
without asking, when that save is safe: the file has not changed elsewhere since
it was opened, you can write it and its folder, it is at most 16 MiB, and the
changes would be written as they are (raw text that does not parse, for
example, would not). A save that still fails is reported as DBFlux quits.

Every other file with unsaved changes is listed in the unsaved changes prompt
before DBFlux quits, and so is every file in object storage, because an upload
can fail or be cut short while the application closes. The prompt works as it
does when you close a tab:

- **Save** saves the checked files. DBFlux quits only once every save succeeded;
  a failed save is reported and DBFlux stays open with your changes. A file you
  left unchecked keeps its changes, and you are asked about it again.
- **Don't save** drops the changes of the listed files and quits.
- **Cancel** keeps DBFlux open and every change.

If a query is running, its prompt comes first, and **Quit anyway** leads to this
one. Before DBFlux quits it checks every open file again, so a file that changed
while you answered is asked about rather than lost.

Stopping DBFlux from a terminal (`Ctrl+c`, or `SIGTERM`) cannot ask: DBFlux
tries to save every local file with unsaved changes and reports a save that
fails, and changes to files in object storage are not uploaded and are recorded
in the log.

## Limits

- **Read-only files.** A local file without a modification time, or an object
  without an ETag or last-modified time, cannot be saved, because a change made
  elsewhere could not be detected. DBFlux shows a warning and hides the edit
  controls. Editing also pauses while a save or a reread runs.
- **Large text.** The text view shows at most 50,000 lines or 8 MiB of the
  loaded records. Beyond that it shows the first records only and cannot be
  edited. The table still shows every loaded record.
- **Refused dialects.** A delimiter or quote that is a line break, a quote equal
  to the delimiter, and a delimiter or quote that can be part of a multi-byte
  character in the chosen encoding (for example the pipe in Shift_JIS) are
  refused with the reason. A file whose detected dialect is refused opens with
  another delimiter and a warning.
- **Text changes that are not kept.** A change to only a line ending or only the
  quoting of a line is not saved, because the values did not change.
- **Columns.** A new column needs the whole file loaded, and always goes after the
  last one. Columns cannot be deleted or reordered, in the table or in the text.
- **Rows after the loaded part.** A row added after the last loaded record of a
  partly loaded file cannot be saved until the next page is loaded.

See the [Keyboard Reference](KEYBOARD.md#csv-and-tsv-files) for every key.
