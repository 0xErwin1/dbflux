# Parquet Files

A `.parquet` file, local or in object storage, opens read-only in its own tab as
a table. DBFlux reads the rows 500 at a time and only the columns you choose,
so a large or wide file opens quickly. A second view lists every column with
the facts the file stores about it in its footer.

## Opening a file

- **Local file.** Open it with `Ctrl+o` (`Cmd+o` on macOS); the dialog has a
  **Parquet Files** filter. Recent files, the Scripts view of the sidebar and
  `dbflux <file>` from a terminal open it the same way. The `.parquet`
  extension decides, in any letter case.
- **Object storage.** In the object browser, choose **Open in editor** in the
  object's menu (`m`) or in its preview header.

A header above the table gives the file's column count, row count and size on
disk. For an object it also says whether the object is read by range or was
downloaded whole.

### Objects and ranged reads

| Store | How the object is read |
|-------|------------------------|
| A store that can read part of an object (S3 today) | Only the bytes each page needs are read, and the object is checked for changes before each page. |
| Any other store | DBFlux first asks whether to download the whole object and shows its size. Declining closes the tab. |

A downloaded object is kept in memory up to 64 MiB and in a temporary file
above that, which is removed when the tab closes. Every page is read from that
copy. **Reload from file** then checks whether the object changed: if it did
not, a notice says so, and if it did, DBFlux asks again before downloading it.

## Reading rows

The table shows the rows in file order. **Load more** (`]`) in the footer reads
the next 500, and the footer counts the rows and columns shown.

Before each page DBFlux checks that the file was not changed since it was
opened. If it was, no more rows are read and the footer says so. **Reload from
file** (`F5`) opens the file again from its first row.

The table takes the [data table keys](KEYBOARD.md#data-table) for moving and
copying. Nothing can be edited.

## Choosing columns

A file opens with its leading columns: the first column always, then the next
ones while one page of 500 rows stays within 16 MiB, up to 32 columns. Each
column header shows its type and, from the footer, its compression ratio, size
on disk and share of nulls.

The **N of M columns** button in the toolbar (`f`) chooses the columns to read.

| Key | In the column picker |
|-----|----------------------|
| Type in the search field | Filters the columns by name or type. |
| `Space` | Ticks or unticks the highlighted column. |
| `Enter` or **Apply** | Reads the file again from its first row with the ticked columns. |
| `Escape` | Closes the picker and drops the changes. |

**Apply** is disabled while no column is ticked. A strip under the toolbar says
what the next page will read, in bytes and row groups, and which columns take
most of it.

## The Columns view

`t` switches between **Data** and **Columns**. The Columns view has one row per
column of the file, with the facts from the footer. A source line above it
names the footer and when the file was last modified, for example "Sizes from
the file footer · file modified 14:22:05". The time is shown for a file
modified today, and the date and time otherwise. A file whose modification
time is unknown shows only "Sizes from the file footer".

| Field | What it shows |
|-------|---------------|
| **Column** | The name, the Parquet type, and a **Sort key N** badge for a column of the sort key. |
| **Codec** | The compression codec, or `MIXED` when the column's chunks use more than one. |
| **On disk**, **Ratio** | The compressed size and the compression ratio. |
| **Share of table** | The column's part of the file's size. |
| **Nulls** | The share of null values, when the file records the null count. |
| **Distribution** | The smallest and largest value, when the file records them. |
| **Distinct** | The distinct count, which most files do not record. |

Above the columns, a strip lists the file's facts:

| Fact | What it shows |
|------|---------------|
| **Row groups** | How many row groups the file has. |
| **Written by** | The writer the footer names (`created_by`), when it names one. |
| **Sort key** | The columns the writer declared the rows sorted by (`sorting_columns`). Shown only when every row group declares the same order. |
| **Rows**, **On disk**, **Raw**, **Ratio** | The row count, the compressed and uncompressed size, and the compression ratio. |

A file whose row groups declare no sort order, or different ones, has no sort
key fact and no **Sort key N** badges, because its rows are not known to be
sorted across the file.

- The eye on each row adds the column to the Data view or drops it, at once.
  `Space` toggles the row under the cursor. The last shown column cannot be
  dropped.
- `Shift+T` cycles the sort: table order, size, name.
- `/` filters the rows by column name or type.

The picker and the Columns view always show the same columns.

## How values are shown

| Type | Shown as |
|------|----------|
| Integers, floats | Numbers. A UInt64 above the largest signed 64-bit value is shown as text with its exact digits. |
| Decimals | Exact text with the scale applied, never rounded. |
| Dates, times | `YYYY-MM-DD` and `HH:MM:SS`, with as many fraction digits as the unit holds. |
| Timestamps adjusted to UTC | In UTC, ending in `Z`. |
| Timestamps in a named zone or with an offset | In UTC, followed by the zone in brackets, for example `2024-05-01 12:00:00.000000Z [Europe/Berlin]`. |
| Timestamps without a zone, and INT96 | As written, without a zone. The header names INT96. |
| Binary | A hex preview of the first 16 bytes and the size, for example `0x0a1b… (40 bytes)`. A 16-byte UUID shows in its usual form. |
| Lists, structs, maps | JSON, cut after 256 characters. |
| VARIANT, GEOMETRY, GEOGRAPHY and other types without a rule | `<unsupported: TYPE>`. |

Files compressed with snappy, zstd, LZ4 or gzip open. A file with a column
compressed with Brotli or LZO is refused with a message that names the column
and the codec.

## Session restore

A local Parquet tab is open again after a restart, showing the default columns.
Objects in storage are not reopened, because they need their connection.

## Limits

- **Read-only.** Rows cannot be edited, and a file cannot be saved or exported
  to Parquet.
- **No sorting or filtering of rows.** The rows stay in file order. A click on a
  column header does not sort.
- **Encrypted files.** A file whose footer is encrypted is refused when it
  opens. A file encrypted with a plaintext footer is not detected when it
  opens, and reading a page fails with "The file is not valid Parquet."
- **Legacy INTERVAL columns.** Months are not read, and the type name says so:
  `INTERVAL (months not read)`.
- **Partial page index.** A file with an offset index for some row groups and
  not for others is refused as not valid Parquet when a page is read.
- **Files without a page index.** Any page of such a file reads whole column
  chunks. A column chunk over 64 MiB is refused with a message that names the
  column.
- **Footer size.** A footer or page index over 16 MiB is refused.
- **Sideways scrolling in the Columns view.** It uses the mouse wheel, the
  trackpad or the scrollbar. There is no key for it yet.
- **Session restore.** The columns you chose are not restored.
