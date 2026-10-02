# dbflux_delimited

Dialect model, dialect detection and text decoding for delimited text files such as CSV and TSV.
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

## Limitations

- The crate does not read records, index them, or write files yet. It only resolves the dialect
  and decodes text.
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
