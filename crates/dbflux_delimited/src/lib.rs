//! Dialect model, dialect detection, text decoding, paged record reading and
//! byte-preserving writing for delimited text files such as CSV and TSV.
//!
//! The crate resolves how a file is laid out (delimiter, quote character, header
//! row, text encoding) from a leading byte sample, lets a caller override any
//! part of that result, and decodes bytes to text under the resolved encoding.
//! [`PagedReader`] then reads the records of a [`ByteSource`] one page at a
//! time, each with its exact byte range in the source. [`write_edited`] writes
//! the source again with an [`EditSet`] applied, copying every record that was
//! not edited byte for byte.
//!
//! ```
//! use dbflux_delimited::{DialectOverrides, SampleCoverage, decode, detect_dialect};
//!
//! let bytes = b"name;city\nJos\xE9;M\xE1laga\nMar\xEDa;C\xF3rdoba\n";
//!
//! let detected = detect_dialect(bytes, SampleCoverage::WholeFile, Some("csv"));
//! assert_eq!(detected.delimiter, b';');
//! assert!(detected.has_header);
//!
//! let overrides = DialectOverrides {
//!     has_header: Some(false),
//!     ..DialectOverrides::default()
//! };
//! let dialect = overrides.apply(detected);
//! assert!(!dialect.has_header);
//!
//! let decoded = decode(bytes, &dialect);
//! assert!(decoded.text.starts_with("name;city\nJosé;Málaga"));
//! assert!(!decoded.had_replacements);
//! ```

use std::borrow::Cow;
use std::collections::BTreeMap;

use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
pub use encoding_rs::Encoding;
use encoding_rs::UTF_8;

mod reader;
mod source;
mod writer;

pub use reader::{Page, PagedReader, ReadError, ReaderOptions, Record, RecordCount};
#[cfg(any(unix, windows))]
pub use source::FileSource;
pub use source::{ByteSource, MemorySource, SourceError};
pub use writer::{
    AppendedColumn, EditLocation, EditSet, InsertPosition, Insertion, Replacement, WriteError,
    write_edited,
};

#[cfg(test)]
mod reader_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod writer_tests;

/// Delimiters that detection chooses between, in the order that decides a tie
/// no other rule settles.
const CANDIDATE_DELIMITERS: [u8; 4] = [b',', b'\t', b';', b'|'];

/// The quote character detection always reports and scans with.
const DEFAULT_QUOTE: u8 = b'"';

/// How a delimited text file is laid out.
///
/// `delimiter` and `quote` are single bytes, which is what the `csv` crate
/// accepts. In a UTF-16 file they name the ASCII character, not its two-byte
/// encoded form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dialect {
    /// The byte that separates two fields of a record.
    pub delimiter: u8,

    /// The byte that encloses a field containing the delimiter, a line break or
    /// the quote itself, or `None` when the file does not quote fields.
    pub quote: Option<u8>,

    /// Whether the first record names the columns instead of holding data.
    pub has_header: bool,

    /// The character encoding of the file's bytes.
    pub encoding: &'static Encoding,
}

/// User choices that replace parts of a detected [`Dialect`].
///
/// Every field is optional and `None` keeps the detected value. `quote` is
/// doubly optional so that "no override" (`None`) stays distinct from "override
/// to a file without quoting" (`Some(None)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DialectOverrides {
    pub delimiter: Option<u8>,
    pub quote: Option<Option<u8>>,
    pub has_header: Option<bool>,
    pub encoding: Option<&'static Encoding>,
}

impl DialectOverrides {
    /// Returns `detected` with every overridden field replaced.
    ///
    /// An override always wins over detection. The fields are independent:
    /// overriding the delimiter or the encoding does not re-run header
    /// detection.
    pub fn apply(&self, detected: Dialect) -> Dialect {
        Dialect {
            delimiter: self.delimiter.unwrap_or(detected.delimiter),
            quote: self.quote.unwrap_or(detected.quote),
            has_header: self.has_header.unwrap_or(detected.has_header),
            encoding: self.encoding.unwrap_or(detected.encoding),
        }
    }
}

/// Text decoded from a file's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedText<'a> {
    /// The decoded text. It borrows from the input when the bytes were already
    /// valid UTF-8 and needed no change.
    pub text: Cow<'a, str>,

    /// Whether at least one malformed byte sequence was replaced with U+FFFD,
    /// which means the text does not round-trip to the original bytes and the
    /// encoding is probably wrong.
    pub had_replacements: bool,
}

/// How much of a file the bytes given to [`detect_dialect`] cover.
///
/// Detection cannot tell a file that ends without a line break from a sample
/// cut in the middle of a record, or a legacy-encoded file from UTF-8 cut
/// inside a character, so the caller states which one it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleCoverage {
    /// The sample is the entire file. Its end is the end of the data.
    WholeFile,

    /// The sample is the start of a longer file and may end inside a record or
    /// inside a multi-byte character.
    Prefix,
}

/// Detects the dialect of a delimited file from a leading byte `sample`.
///
/// `coverage` says whether `sample` is the whole file or only its start.
/// `extension` is the file extension, with or without the leading dot and in
/// any letter case. Only `tsv` (tab) and `csv` (comma) carry a hint.
///
/// # Encoding
///
/// A UTF-8, UTF-16 LE or UTF-16 BE byte-order mark decides the encoding.
/// Without one, a sample that is valid UTF-8 is UTF-8. A
/// [`SampleCoverage::Prefix`] sample whose only invalid part is an incomplete
/// multi-byte character at its very end is also UTF-8, because the cut explains
/// it. Anything else, including a [`SampleCoverage::WholeFile`] sample with
/// that same incomplete ending, goes through a statistical detector, which
/// guesses among legacy encodings such as windows-1252 and never reports
/// UTF-16.
///
/// # Delimiter
///
/// The sample is decoded and split into records, keeping line breaks inside
/// double-quoted fields within their record. A line feed, a carriage return or
/// the pair ends a record, and empty records are skipped. When the sample ends
/// inside a quoted field and no record was terminated before that, the quote
/// is taken as unbalanced and the sample is split again at every line break.
///
/// The text after the last line break is the final record of a
/// [`SampleCoverage::WholeFile`] sample. In a [`SampleCoverage::Prefix`]
/// sample it is treated as a record cut off by the end of the sample and is
/// ignored, unless it is the only record there is.
///
/// For each of comma, tab, semicolon and pipe, the delimiters outside quoted
/// fields are counted per record. The most frequent count is the candidate's
/// field count, and a candidate whose most frequent count is zero is dropped.
/// The winner is the candidate whose field count is shared by the most
/// records. A tie goes to the extension hint, then to the candidate with more
/// fields per record, then to the order comma, tab, semicolon, pipe. When
/// every candidate is dropped (an empty or single-column sample) the hint
/// decides, and comma is the fallback.
///
/// # Quote
///
/// The quote is always reported as the double quote. Other quote characters
/// are reachable only through [`DialectOverrides`].
///
/// # Header
///
/// The heuristic looks only at the first record, split with the detected
/// delimiter. A field is numeric when, after trimming whitespace and one pair
/// of surrounding double quotes, it contains an ASCII digit and parses as a
/// decimal number (`42`, `-3.5`, `1e9`). The file is reported as having no
/// header only when the first record has at least one numeric field. In every
/// other case, including an empty sample and a file whose records are all
/// text, a header is reported: a data row wrongly shown as a header is easier
/// to notice and override than a header wrongly shown as data.
pub fn detect_dialect(sample: &[u8], coverage: SampleCoverage, extension: Option<&str>) -> Dialect {
    let encoding = detect_encoding(sample, coverage);

    let (text, _) = encoding.decode_with_bom_removal(sample);
    let records = sample_records(&text, coverage);

    let delimiter = detect_delimiter(&records, extension.and_then(delimiter_for_extension));
    let has_header = detect_header(records.first().copied(), delimiter);

    Dialect {
        delimiter,
        quote: Some(DEFAULT_QUOTE),
        has_header,
        encoding,
    }
}

/// Decodes `bytes` to text under the encoding of `dialect`.
///
/// A leading byte-order mark of that same encoding is dropped. A byte-order
/// mark of a different encoding is decoded as ordinary content and never
/// switches the encoding, so the dialect stays the single source of truth.
/// Malformed sequences become U+FFFD and are reported through
/// [`DecodedText::had_replacements`].
///
/// `bytes` should start and end on a character boundary. A slice that cuts a
/// multi-byte character reports a replacement for the partial character.
pub fn decode<'a>(bytes: &'a [u8], dialect: &Dialect) -> DecodedText<'a> {
    let (text, had_replacements) = dialect.encoding.decode_with_bom_removal(bytes);

    DecodedText {
        text,
        had_replacements,
    }
}

fn detect_encoding(sample: &[u8], coverage: SampleCoverage) -> &'static Encoding {
    if let Some((encoding, _bom_length)) = Encoding::for_bom(sample) {
        return encoding;
    }

    match std::str::from_utf8(sample) {
        Ok(_) => UTF_8,

        // No error length means the input ended inside a character. In a
        // prefix the cut explains it, and everything before it is valid.
        Err(error) if coverage == SampleCoverage::Prefix && error.error_len().is_none() => UTF_8,

        Err(_) => {
            let is_end_of_stream = coverage == SampleCoverage::WholeFile;

            let mut detector = EncodingDetector::new(Iso2022JpDetection::Deny);
            detector.feed(sample, is_end_of_stream);
            detector.guess(None, Utf8Detection::Deny)
        }
    }
}

fn delimiter_for_extension(extension: &str) -> Option<u8> {
    let extension = extension.trim_start_matches('.');

    if extension.eq_ignore_ascii_case("tsv") {
        Some(b'\t')
    } else if extension.eq_ignore_ascii_case("csv") {
        Some(b',')
    } else {
        None
    }
}

/// The outcome of one pass over sample text looking for record boundaries.
struct SplitRecords<'a> {
    /// The non-empty records that end in a line break.
    terminated: Vec<&'a str>,

    /// The text after the last line break, possibly empty.
    unterminated: &'a str,

    /// Whether the text ended inside a double-quoted field.
    ended_in_quotes: bool,
}

/// Returns the records of sample text that detection should count.
///
/// An unbalanced quote that swallows the whole sample is recovered from by
/// splitting again without quote awareness. The unterminated final record is
/// kept for a whole file, and for a prefix only when it is the only record.
fn sample_records(text: &str, coverage: SampleCoverage) -> Vec<&str> {
    let quote_aware = split_records(text, true);

    let split = if quote_aware.ended_in_quotes && quote_aware.terminated.is_empty() {
        split_records(text, false)
    } else {
        quote_aware
    };

    let mut records = split.terminated;

    let keeps_unterminated = match coverage {
        SampleCoverage::WholeFile => true,
        SampleCoverage::Prefix => records.is_empty(),
    };

    if keeps_unterminated && !split.unterminated.is_empty() {
        records.push(split.unterminated);
    }

    records
}

/// Splits text at every line feed or carriage return. With `quote_aware`, a
/// line break inside a double-quoted field stays within its record.
fn split_records(text: &str, quote_aware: bool) -> SplitRecords<'_> {
    let mut terminated = Vec::new();
    let mut in_quotes = false;
    let mut record_start = 0;

    for (index, character) in text.char_indices() {
        match character {
            '"' if quote_aware => in_quotes = !in_quotes,

            '\n' | '\r' if !in_quotes => {
                if let Some(record) = text.get(record_start..index)
                    && !record.is_empty()
                {
                    terminated.push(record);
                }

                record_start = index + character.len_utf8();
            }

            _ => {}
        }
    }

    SplitRecords {
        terminated,
        unterminated: text.get(record_start..).unwrap_or_default(),
        ended_in_quotes: in_quotes,
    }
}

/// Splits one record at every `delimiter` outside a double-quoted field. The
/// fields keep their quotes.
fn split_fields(record: &str, delimiter: u8) -> Vec<&str> {
    let delimiter = char::from(delimiter);

    let mut fields = Vec::new();
    let mut in_quotes = false;
    let mut field_start = 0;

    for (index, character) in record.char_indices() {
        if character == '"' {
            in_quotes = !in_quotes;
        } else if character == delimiter && !in_quotes {
            fields.extend(record.get(field_start..index));
            field_start = index + character.len_utf8();
        }
    }

    fields.extend(record.get(field_start..));

    fields
}

fn detect_delimiter(records: &[&str], hint: Option<u8>) -> u8 {
    let mut best: Option<(u8, (usize, bool, usize))> = None;

    for candidate in CANDIDATE_DELIMITERS {
        let Some((delimiters_per_record, agreeing_records)) =
            most_frequent_delimiter_count(records, candidate)
        else {
            continue;
        };

        if delimiters_per_record == 0 {
            continue;
        }

        let score = (
            agreeing_records,
            hint == Some(candidate),
            delimiters_per_record,
        );

        if best.is_none_or(|(_, best_score)| score > best_score) {
            best = Some((candidate, score));
        }
    }

    best.map(|(delimiter, _)| delimiter)
        .or(hint)
        .unwrap_or(b',')
}

/// Returns the per-record delimiter count shared by the most records, with the
/// number of records that share it. Two counts shared equally often resolve to
/// the larger count.
fn most_frequent_delimiter_count(records: &[&str], delimiter: u8) -> Option<(usize, usize)> {
    let mut frequencies: BTreeMap<usize, usize> = BTreeMap::new();

    for record in records {
        let delimiter_count = split_fields(record, delimiter).len().saturating_sub(1);
        *frequencies.entry(delimiter_count).or_default() += 1;
    }

    frequencies
        .into_iter()
        .max_by_key(|&(delimiter_count, frequency)| (frequency, delimiter_count))
}

fn detect_header(first_record: Option<&str>, delimiter: u8) -> bool {
    let Some(first_record) = first_record else {
        return true;
    };

    !split_fields(first_record, delimiter)
        .into_iter()
        .any(is_numeric_field)
}

fn is_numeric_field(field: &str) -> bool {
    let field = field.trim();

    let unquoted = field
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or(field)
        .trim();

    unquoted.contains(|character: char| character.is_ascii_digit())
        && unquoted.parse::<f64>().is_ok()
}
