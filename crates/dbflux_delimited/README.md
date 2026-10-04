# dbflux_delimited

Dialect model, dialect detection, text decoding and paged record reading for delimited text files
such as CSV and TSV.
The crate has no dependency on `dbflux_core` or on any UI crate.

## Features

- `Dialect` describes a file's layout: field delimiter, quote character (or none), whether the
  first record is a header, and the text encoding. It is `Copy` and comparable for equality.
- `detect_dialect` resolves a `Dialect` from a leading byte sample and an optional file-extension
  hint. The caller states through `SampleCoverage` whether the sample is the whole file or only its
  start, which decides how the end of the sample is read.
  - Encoding: a UTF-8, UTF-16 LE or UTF-16 BE byte-order mark decides. Without one, valid UTF-8 is
    UTF-8, and anything else goes through a statistical detector for legacy encodings such as
    windows-1252. A prefix that ends inside a multi-byte UTF-8 character is still UTF-8. A whole
    file that ends that way is not.
  - Delimiter: comma, tab, semicolon or pipe, whichever gives the most consistent field count
    across the sample's records. Delimiters and line breaks inside double-quoted fields are not
    counted. The final record of a prefix is ignored as cut off, and the final record of a whole
    file is counted even without a trailing line break. A sample swallowed by one unbalanced quote
    is split again at every line break. The `csv` and `tsv` extensions break ties and decide for
    empty or single-column samples.
  - Header: reported as absent only when the first record has a numeric field, and as present in
    every other case.
- `DialectOverrides` replaces any of the four detected values. An override always wins.
- `decode` turns bytes into text under a dialect's encoding, drops that encoding's byte-order
  mark, and reports whether a malformed sequence was replaced with U+FFFD.
- `ByteSource` is the trait the reader reads through: a total length and a bounded read of a byte
  range, both blocking and fallible. `SourceError` wraps the caller's own error and keeps its
  message. `MemorySource` reads bytes held in memory and `FileSource` reads a local file with
  positioned reads.
- `PagedReader` reads the records of a source one page at a time, with a caller-chosen page size.
  - Records are found in the raw bytes: a quote at the start of a field opens a quoted field, a
    doubled quote inside it is one literal quote, and a record ends at a line feed, a carriage
    return or the pair outside quotes. A final record without a terminator is a record, and an
    empty line is a record with one empty field. With no quote character only the terminators are
    special. Field splitting runs the same state machine as record scanning.
  - UTF-16 LE and BE are scanned in two-byte code units, without transcoding the file. Every other
    encoding is scanned by byte.
  - Each record carries its byte range in the source, terminator included, its decoded fields, and
    whether decoding replaced a malformed sequence. The byte-order mark, the header and the records
    are contiguous and cover the whole source.
  - The index keeps one byte offset per scanned page. Reading a page scans forward from the last
    indexed page and remembers every page start it passes, so a later read of any indexed page
    goes straight to its offset. Reading the page that follows the last one read reuses the bytes
    already fetched.
  - `record_count` reports the exact total once a scan reached the end of the source, and the
    number of records indexed so far until then.
  - With a header, the first record is exposed through `header` and is excluded from pages and
    counts. A byte-order mark of the dialect's encoding is skipped and belongs to no record.
  - The source is read in windows of a configurable size. A record that does not end within the
    fetched bytes is continued with further reads, each double the previous one, and is never
    returned before its end is settled.
  - `invalidate_from_offset` and `invalidate_from_page` discard the index after a byte offset or a
    page start and read the source's length again. Offset zero and page zero also read the
    byte-order mark and the header again.

## Limitations

- The crate does not write files yet.
- The reader refuses a dialect whose delimiter or quote byte can be part of a multi-byte
  character, instead of scanning it wrongly: a non-ASCII byte in UTF-8 or EUC-JP, a byte from 0x30
  to 0x39 or from 0x40 up (which includes the pipe) in Shift_JIS, GBK, gb18030, Big5 and EUC-KR,
  and any byte in ISO-2022-JP, which cannot be read at all.
- The reader refuses a delimiter or quote that is a line feed or a carriage return, and a quote
  equal to the delimiter.
- The reader's quoting rules are stricter than the ones detection counts with: detection treats
  every quote as opening or closing a quoted field, and the reader only opens one at the start of
  a field.
- A whitespace-padded quoted field (`a, "b"`) is read as unquoted text that contains quotes.
- A file with a different number of fields per record is read as it is. The reader does not pad
  or check field counts.
- A page read fetches at least one window even when the page is smaller, and a jump to a page that
  is not indexed yet fetches every byte between the last indexed page and it.
- The reader holds a whole page of decoded records, and a whole record, in memory. A single record
  larger than the available memory cannot be read.
- The source's length is read when the reader opens and on invalidation. A source that changes in
  between without an invalidation is read with stale offsets.
- A dialect change needs a new reader over the same source.
- `FileSource` exists on Unix and Windows only.
- Detection always reports the double quote as the quote character. A single-quoted or unquoted
  file needs an override.
- Only comma, tab, semicolon and pipe are detected. Any other delimiter needs an override, and the
  delimiter and quote are single bytes.
- UTF-16 is detected only through its byte-order mark. Without one, UTF-16 text that is all ASCII
  is valid UTF-8 with NUL bytes and is reported as UTF-8, and any other UTF-16 text is guessed as a
  legacy encoding. Both need the encoding override.
- A headerless file with as many decimal commas as semicolons per record (for example
  `a;1,5` then `b;2,5`) ties between comma and semicolon. The tie resolves by the extension hint,
  then the candidate order, so such a file may need the delimiter override.
- Encoding detection without a byte-order mark is statistical. A short sample, or one with few
  non-ASCII bytes, can be guessed wrong.
- Header detection is a heuristic over the first record only. A headerless file whose first record
  is all text is reported as having a header, and a header row with a numeric column name (for
  example a year) is reported as data.
- Applying an override does not re-run detection: overriding the delimiter or the encoding keeps
  the header guess made with the detected values.
- `decode` expects bytes that start and end on a character boundary. A slice that cuts a
  multi-byte character reports a replacement.
