# Spreadsheet Files

An `.xlsx`, `.xlsm`, `.xls` or `.ods` workbook, local or in object storage,
opens in its own tab with one tab per sheet. Each sheet is shown as a table,
and a second view shows its values as CSV text. You can edit cells and add rows
at the end of a sheet in xlsx, xlsm and ods files. A save changes only the parts
of the file that hold your edits and keeps everything else as it is.

## Opening a file

- **Local file.** Open it with `Ctrl+o` (`Cmd+o` on macOS); the dialog has a
  **Spreadsheet Files** filter. Recent files, the Scripts view of the sidebar
  and `dbflux <file>` from a terminal open it the same way. The extension
  decides, in any letter case.
- **Object storage.** In the object browser, choose **Open in editor** in the
  object's menu (`m`) or in its preview header.

DBFlux then recognizes the format from the file's content, not from its name:

| Format | How it opens |
|--------|--------------|
| xlsx, xlsm | Editable. Formulas are shown as written, starting with `=`. |
| ods | Editable. Formulas are shown in OpenFormula, for example `of:=[.B2]*3`. |
| xls | Read-only, with **Save as .xlsx**. Formulas are not available. |

A workbook encrypted or protected with a password to open is refused with a
message.

The header above the table gives the sheet count, the size of the shown sheet
and a reminder that the whole sheet is loaded in memory.

## Sheets

The sheet tabs are below the table. Click a tab, or use `Alt+l` and `Alt+h`, to
show the next or previous sheet; both wrap around. The tab of a hidden sheet is
marked **hidden**, and a sheet with unsaved edits ends its name with `•`.

A chart sheet holds a chart and no cells. Its tab is listed, marked **chart**,
but it cannot be opened, and `Alt+l` and `Alt+h` skip it. The workbook opens on
its first visible sheet.

DBFlux reads a whole sheet at once and keeps only the shown sheet in memory:
switching sheets drops the shown one and reads the next. A sheet larger than
10,000,000 cells, counted from cell A1 to its last row and column, is refused
with a message that gives its size.

## The table

Columns are titled with letters (A, B, …, Z, AA, …) and row 1 is a row of data
like any other, so the grid matches what a spreadsheet application shows. The
grid starts at A1 even when the first value is further down. Each column header
names what its cells hold: `integer`, `number`, `date` or `text`.

The table takes the [data table keys](KEYBOARD.md#data-table) for moving and
copying.

### The formula bar

Above the table, the formula bar shows the formula of the selected cell, or
**No formula**. In an xls file it shows **Formula not available for .xls files**
for every cell that holds a value, because DBFlux cannot read xls formulas
correctly. The table always shows a cell's value, never its formula.

## Editing

`Enter` or `F2` edits a cell, as in any [data table](KEYBOARD.md#data-table).
`Ctrl+n` clears a cell, because a sheet has no NULL.

- **Append row** in the edit bar, or `a a`, adds an empty row after the last
  row of the sheet. It is turned off for a sheet whose last row DBFlux cannot
  work out, such as one that names a table part the file does not hold: its
  cells are still shown, and saving edits to it fails for the same reason.
- `d d` deletes a row only while it is an appended row that is not saved yet.
- Inserting, duplicating or deleting any other row is refused with a message:
  it would move the cells that formulas, charts and ranges point at.
- An empty sheet of an editable file is shown as a grid of 10 empty columns,
  so you can append rows to it.

Each sheet keeps its own pending edits when you switch to another sheet.

### What a typed value becomes

| You type | It is written as |
|----------|------------------|
| An empty value | A cleared cell. |
| `'` followed by anything | Text, without the `'`. |
| `=` followed by a formula | A formula. |
| `true` or `false`, in any case | A boolean, in a cell that already holds a boolean. Text elsewhere. |
| An ISO date, such as `2025-03-04` or `2025-03-04 10:30:00` | A date, in a cell whose number format is a date format. Text elsewhere. |
| A number, such as `42`, `-1.5` or `1e3` | A number. |
| Anything else | Text. |

In an ods file a formula must be OpenFormula: write references in brackets,
such as `[.A1]` or `[.A1:.B3]`. A formula with an A1-style reference such as
`B2` is refused at save; fix it, or start the value with `'` to store it as
text.

When your edits replace formulas with values, the edit bar says how many. No
other confirmation is asked: undo the edit (`u` or `Ctrl+z`) to keep the
formula.

## Saving

`Ctrl+s` (`Cmd+s` on macOS) or **Save** in the edit bar saves every sheet's
edits into the same file. `Ctrl+Enter` saves from the table.

- **Only the edited parts change.** Every other part of the file is copied
  byte for byte, so styles, shared strings, drawings, charts, images,
  comments, tables and macros are kept:

  | Format | Parts a save changes |
  |--------|----------------------|
  | xlsx, xlsm | The worksheet parts you edited, and `xl/workbook.xml`, which gets the flag that asks for a full recalculation. When the file has a calculation chain, `xl/calcChain.xml` is removed with its relationship in `xl/_rels/workbook.xml.rels` and its entry in `[Content_Types].xml`. |
  | ods | Only `content.xml`, which holds the cells of every sheet. |

- **An edited cell keeps its style.** In xlsx and xlsm, a cell of an appended
  row takes the style of the cell above it in its column.
- **The file is checked first.** A save is refused when the file changed
  elsewhere since it was opened. Your edits are kept.
- **A refused value refuses the whole save**, and the file is not touched.
  The message names the cell. A save is refused for:
  - in xlsx, a cell that holds a shared formula other cells reuse;
  - in ods, a cell covered by a merged range;
  - text of more than 32,767 characters;
  - a cell inside the range an array formula fills;
  - a character a spreadsheet file cannot store;
  - a date the file cannot store.

After a save, DBFlux reads the file again and shows the same sheet.

Quitting with unsaved edits works as it does for
[CSV files](CSV_FILES.md#quitting-with-unsaved-changes). The save DBFlux makes
on its own as it quits differs from a regular save in one way: it runs the same
checks and writes your pending edits, but does not read the file again
afterwards.

### Recalculation

DBFlux does not compute formulas. A save leaves that to spreadsheet
applications:

- **xlsx and xlsm.** The file asks for a full recalculation when it is opened.
  Excel does this. LibreOffice does not by default, so values that depend on
  an edited cell can look stale there until you recalculate with
  `Ctrl+Shift+F9`, or set LibreOffice to always recalculate Excel files on
  load.
- **ods.** The saved file drops the cached result of every formula, so
  LibreOffice computes them when it opens the file.

A formula without a cached result, such as an ods formula after a save or a
formula you typed into an xlsx file, shows **Recalculated when opened** in the
table until a spreadsheet application saves the file again. The formula bar
still shows its formula, and the text view shows an empty field for it.

## xls files

DBFlux cannot write the xls format, so an xls file is read-only. **Save as
.xlsx** in its banner, or `Ctrl+s`, writes the values of every sheet into a new
xlsx file that you can edit, after a prompt that says what the new file does
not keep:

- **Kept:** every worksheet under its name and in its order, numbers, text,
  booleans, dates, error values as their text, and hidden sheets as hidden.
- **Not kept:** formulas (each becomes the value it shows), formatting, number
  formats other than dates, merged cells, column widths, charts, images and
  macros. Chart sheets are not written.

The original xls file is never changed, and choosing it as the target is
refused. For an object, the new file is saved on this computer and not
uploaded. The new file opens in a tab.

## The text view

`t` switches between **Table** and **Text**. The text view shows the shown
sheet's values as standard CSV, for copying and searching: comma-separated, one
record per row, pending edits included. A value that holds a comma, a double
quote or a line break is quoted, so a multi-line value is written as one quoted
field over several lines. It shows values, never formulas. It is read-only and
has no save of its own.

The text stops at 8 MiB or 50,000 lines, and a note below it says how many of
the sheet's rows it shows. The table still shows every row.

While you are in the text, `Escape` gives the keyboard back to the tab and
`Enter` returns to the text.

## Objects in storage

| Store | How the object is read |
|-------|------------------------|
| A store that can read part of an object (S3 today) | Only the bytes each sheet needs are read. |
| Any other store | DBFlux first asks whether to download the whole object and shows its size. Declining closes the tab. |

A downloaded object is kept in memory up to 64 MiB and in a temporary file
above that. A save writes the edited file to a temporary file, checks that the
object did not change since it was opened, and uploads it. Saves of objects
are recorded in the [audit log](AUDIT.md).

## Session restore

A local spreadsheet tab is open again after a restart, on its first sheet and
without pending edits. Objects in storage are not reopened, because they need
their connection.

## Limitations

- **Rows only at the end.** Rows cannot be inserted or deleted inside a sheet,
  and no column can be added.
- **Tables and merged ranges are not grown.** A row appended under a table or a
  merged range is not added to it.
- **No per-cell styling.** Fonts, colors, borders and number formats are kept
  but cannot be changed.
- **Undo history.** Switching sheets keeps a sheet's pending edits but drops its
  undo history.
- **Typing the shown value keeps the formula.** Typing into a formula cell the
  value it already shows is not an edit, so the formula stays.
- **xls formulas.** They cannot be read, so they are neither shown nor kept by
  Save as .xlsx.
- **LibreOffice and xlsx.** LibreOffice does not recalculate xlsx files on load
  by default, so it can show stale values after a save.
- **Memory.** The whole shown sheet is held in memory, up to 10,000,000 cells.
