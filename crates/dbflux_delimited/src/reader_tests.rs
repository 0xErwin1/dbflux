#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::cell::RefCell;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;

use encoding_rs::{
    BIG5, EUC_JP, EUC_KR, Encoding, GB18030, GBK, ISO_2022_JP, SHIFT_JIS, UTF_8, UTF_16BE,
    UTF_16LE, WINDOWS_1252,
};

use super::reader::never_collides_with_a_trail_byte;
use super::{
    ByteSource, Dialect, FileSource, MemorySource, PagedReader, ReadError, ReaderOptions, Record,
    RecordCount, SourceError,
};

/// An in-memory source that remembers every range it was asked for.
struct RecordingSource {
    bytes: Vec<u8>,
    requests: RefCell<Vec<Range<u64>>>,
}

impl RecordingSource {
    fn new(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
            requests: RefCell::new(Vec::new()),
        }
    }

    fn take_requests(&self) -> Vec<Range<u64>> {
        self.requests.take()
    }
}

impl ByteSource for RecordingSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(self.bytes.len() as u64)
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        self.requests.borrow_mut().push(range.clone());

        let end = (range.end as usize).min(self.bytes.len());
        let start = (range.start as usize).min(end);

        Ok(self.bytes[start..end].to_vec())
    }
}

/// A source that fails every read after `successful_reads` of them.
struct FailingSource {
    bytes: Vec<u8>,
    successful_reads: RefCell<usize>,
    message: &'static str,
}

impl ByteSource for FailingSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(self.bytes.len() as u64)
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let mut remaining = self.successful_reads.borrow_mut();

        if *remaining == 0 {
            return Err(SourceError::new(self.message));
        }

        *remaining -= 1;

        let end = (range.end as usize).min(self.bytes.len());
        Ok(self.bytes[range.start as usize..end].to_vec())
    }
}

fn options(page_size: usize, window_size: u64) -> ReaderOptions {
    ReaderOptions {
        page_size: NonZeroUsize::new(page_size).unwrap(),
        window_size: NonZeroU64::new(window_size).unwrap(),
    }
}

fn dialect(encoding: &'static Encoding) -> Dialect {
    Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: false,
        encoding,
    }
}

fn open(
    bytes: &[u8],
    dialect: Dialect,
    page_size: usize,
    window_size: u64,
) -> PagedReader<RecordingSource> {
    PagedReader::open(
        RecordingSource::new(bytes),
        dialect,
        options(page_size, window_size),
    )
    .unwrap()
}

fn all_records(reader: &mut PagedReader<RecordingSource>) -> Vec<Record> {
    let mut records = Vec::new();

    for page in 0.. {
        let page = reader.read_page(page).unwrap();

        if page.records.is_empty() {
            break;
        }

        records.extend(page.records);
    }

    records
}

fn field_rows(records: &[Record]) -> Vec<Vec<&str>> {
    records
        .iter()
        .map(|record| record.fields.iter().map(String::as_str).collect())
        .collect()
}

/// Reads `bytes` with every window size from one byte to past the end, and
/// checks each time that the byte-order mark, the header and the records tile
/// the source exactly and that the fields match `expected`.
fn assert_reads_as(bytes: &[u8], dialect: Dialect, expected: &[&[&str]]) {
    for window_size in 1..=bytes.len() as u64 + 1 {
        let mut reader = open(bytes, dialect, 2, window_size);
        let records = all_records(&mut reader);

        let mut rebuilt = bytes[..reader.byte_order_mark_length() as usize].to_vec();

        for record in reader.header().into_iter().chain(&records) {
            assert_eq!(
                record.byte_range.start,
                rebuilt.len() as u64,
                "window {window_size}: records are not contiguous"
            );

            rebuilt
                .extend(&bytes[record.byte_range.start as usize..record.byte_range.end as usize]);
        }

        assert_eq!(rebuilt, bytes, "window {window_size}");

        let expected_rows: Vec<Vec<&str>> = expected.iter().map(|row| row.to_vec()).collect();
        assert_eq!(field_rows(&records), expected_rows, "window {window_size}");
    }
}

fn utf16le(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn utf16be(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_be_bytes).collect()
}

#[test]
fn line_feed_records_tile_the_source() {
    assert_reads_as(
        b"a,b\nc,d\ne,f\n",
        dialect(UTF_8),
        &[&["a", "b"], &["c", "d"], &["e", "f"]],
    );
}

#[test]
fn crlf_records_tile_the_source() {
    assert_reads_as(
        b"a,b\r\nc,d\r\ne,f\r\n",
        dialect(UTF_8),
        &[&["a", "b"], &["c", "d"], &["e", "f"]],
    );
}

#[test]
fn bare_carriage_return_records_tile_the_source() {
    assert_reads_as(
        b"a,b\rc,d\re,f\r",
        dialect(UTF_8),
        &[&["a", "b"], &["c", "d"], &["e", "f"]],
    );
}

#[test]
fn mixed_line_endings_tile_the_source() {
    assert_reads_as(
        b"a,b\r\nc,d\ne,f\rg,h\r\ni,j",
        dialect(UTF_8),
        &[
            &["a", "b"],
            &["c", "d"],
            &["e", "f"],
            &["g", "h"],
            &["i", "j"],
        ],
    );
}

#[test]
fn crlf_terminator_belongs_to_its_record() {
    let mut reader = open(b"a\r\nb\r\n", dialect(UTF_8), 10, 2);

    let ranges: Vec<Range<u64>> = all_records(&mut reader)
        .into_iter()
        .map(|record| record.byte_range)
        .collect();

    assert_eq!(ranges, vec![0..3, 3..6]);
}

#[test]
fn bare_carriage_return_followed_by_data_ends_before_the_data() {
    let mut reader = open(b"a\rb\n", dialect(UTF_8), 10, 2);

    let ranges: Vec<Range<u64>> = all_records(&mut reader)
        .into_iter()
        .map(|record| record.byte_range)
        .collect();

    assert_eq!(ranges, vec![0..2, 2..4]);
}

#[test]
fn quoted_field_keeps_its_delimiter() {
    assert_reads_as(
        b"\"x,y\",1\n\"p,q,r\",2\n",
        dialect(UTF_8),
        &[&["x,y", "1"], &["p,q,r", "2"]],
    );
}

#[test]
fn quoted_field_keeps_its_line_breaks() {
    assert_reads_as(
        b"\"one\ntwo\",1\r\n\"three\r\nfour\rfive\",2\r\n",
        dialect(UTF_8),
        &[&["one\ntwo", "1"], &["three\r\nfour\rfive", "2"]],
    );
}

#[test]
fn doubled_quote_is_one_literal_quote() {
    // If `""` closed the field, the line feed after `q` would end the record
    // and the delimiter after it would split the field.
    assert_reads_as(
        b"\"say \"\"q\n,\"\" now\",1\n\"\"\"\",2\n",
        dialect(UTF_8),
        &[&["say \"q\n,\" now", "1"], &["\"", "2"]],
    );
}

#[test]
fn quote_inside_an_unquoted_field_is_literal() {
    // If the quote opened a quoted field, the line feed would not end the record.
    assert_reads_as(
        b"5\" nail,x\nb\"c,y\n",
        dialect(UTF_8),
        &[&["5\" nail", "x"], &["b\"c", "y"]],
    );
}

#[test]
fn text_after_a_closing_quote_stays_in_the_field() {
    assert_reads_as(
        b"\"a,b\"c,d\ne\n",
        dialect(UTF_8),
        &[&["a,bc", "d"], &["e"]],
    );
}

#[test]
fn unterminated_quote_runs_to_the_end_of_the_source() {
    assert_reads_as(
        b"a,b\n\"open,c\nd,e\n",
        dialect(UTF_8),
        &[&["a", "b"], &["open,c\nd,e\n"]],
    );
}

#[test]
fn empty_fields_and_empty_lines_are_kept() {
    assert_reads_as(
        b",a,\n\n\"\",b\n",
        dialect(UTF_8),
        &[&["", "a", ""], &[""], &["", "b"]],
    );
}

#[test]
fn final_record_without_a_terminator_is_a_record() {
    let mut reader = open(b"a,b\nc,d", dialect(UTF_8), 10, 64);
    let records = all_records(&mut reader);

    assert_eq!(field_rows(&records), vec![vec!["a", "b"], vec!["c", "d"]]);
    assert_eq!(records[1].byte_range, 4..7);
    assert_eq!(reader.record_count(), RecordCount::Total(2));
}

#[test]
fn final_terminator_does_not_add_an_empty_record() {
    let mut reader = open(b"a,b\nc,d\n", dialect(UTF_8), 10, 64);
    let records = all_records(&mut reader);

    assert_eq!(records.len(), 2);
    assert_eq!(records[1].byte_range, 4..8);
    assert_eq!(reader.record_count(), RecordCount::Total(2));
}

#[test]
fn without_a_quote_only_terminators_are_special() {
    let unquoted = Dialect {
        quote: None,
        ..dialect(UTF_8)
    };

    assert_reads_as(b"a,\"b\nc\",d\n", unquoted, &[&["a", "\"b"], &["c\"", "d"]]);
}

#[test]
fn record_larger_than_the_window_grows_the_request() {
    let bytes = b"\"0123456789012345678901234567\"\nb\nc\nd\ne\nf\ng\nh\ni\nj\n";
    let mut reader = open(bytes, dialect(UTF_8), 1, 4);

    let page = reader.read_page(0).unwrap();

    assert_eq!(page.records[0].fields, vec!["0123456789012345678901234567"]);
    assert_eq!(page.records[0].byte_range, 0..31);
    assert_eq!(
        reader.source().take_requests(),
        vec![0..4, 4..8, 8..16, 16..32]
    );
}

#[test]
fn record_several_windows_long_is_read_whole() {
    let field = "x".repeat(1000);
    let bytes = format!("a,b\n\"{field}\n{field}\",c\nd,e\n");

    assert_reads_as(
        bytes.as_bytes(),
        dialect(UTF_8),
        &[
            &["a", "b"],
            &[&format!("{field}\n{field}"), "c"],
            &["d", "e"],
        ],
    );

    let mut reader = open(bytes.as_bytes(), dialect(UTF_8), 10, 16);
    reader.read_page(0).unwrap();

    let requests = reader.source().take_requests();
    assert!(requests.len() > 3);
    assert!(
        requests.windows(2).all(|pair| pair[0].end == pair[1].start),
        "windows must not overlap or skip: {requests:?}"
    );
}

#[test]
fn window_boundary_inside_a_utf8_character_does_not_corrupt_it() {
    let bytes = "é,ü\n€,日本\n".as_bytes();

    assert_reads_as(bytes, dialect(UTF_8), &[&["é", "ü"], &["€", "日本"]]);

    let mut reader = open(bytes, dialect(UTF_8), 10, 1);
    assert!(
        all_records(&mut reader)
            .iter()
            .all(|record| !record.had_replacements)
    );
}

#[test]
fn utf16_is_scanned_in_code_units() {
    // U+012C and U+2C00 carry the comma byte, U+010A and U+0A00 the line feed
    // byte, U+010D the carriage return byte and U+2200 the quote byte.
    let text = "Ĭ\u{2C00},Ċ\u{0A00}\nč∀,\"q,\"\"\nr\"\r\nlast";
    let expected: &[&[&str]] = &[&["Ĭ\u{2C00}", "Ċ\u{0A00}"], &["č∀", "q,\"\nr"], &["last"]];

    let mut little_endian = vec![0xFF, 0xFE];
    little_endian.extend(utf16le(text));

    let mut big_endian = vec![0xFE, 0xFF];
    big_endian.extend(utf16be(text));

    assert_reads_as(&little_endian, dialect(UTF_16LE), expected);
    assert_reads_as(&big_endian, dialect(UTF_16BE), expected);
    assert_reads_as(&utf16le(text), dialect(UTF_16LE), expected);
}

#[test]
fn utf16_byte_order_mark_is_not_part_of_the_first_record() {
    let mut bytes = vec![0xFF, 0xFE];
    bytes.extend(utf16le("a,b\r\nc,d\r\n"));

    // Window 3 cuts every other code unit in half.
    let mut reader = open(&bytes, dialect(UTF_16LE), 10, 3);
    let records = all_records(&mut reader);

    assert_eq!(reader.byte_order_mark_length(), 2);
    assert_eq!(records[0].byte_range, 2..12);
    assert_eq!(records[1].byte_range, 12..22);
    assert_eq!(field_rows(&records), vec![vec!["a", "b"], vec!["c", "d"]]);
    assert!(records.iter().all(|record| !record.had_replacements));
}

#[test]
fn utf16_trailing_odd_byte_stays_in_the_final_record_and_is_reported() {
    let mut bytes = utf16le("a\nb");
    bytes.push(0x41);

    let mut reader = open(&bytes, dialect(UTF_16LE), 10, 4);
    let records = all_records(&mut reader);

    assert_eq!(records[1].byte_range, 4..7);
    assert_eq!(records[1].fields, vec!["b\u{FFFD}"]);
    assert!(records[1].had_replacements);
    assert!(!records[0].had_replacements);
}

#[test]
fn utf8_byte_order_mark_is_not_part_of_the_first_record() {
    let mut reader = open(b"\xEF\xBB\xBFa,b\nc,d\n", dialect(UTF_8), 10, 64);
    let records = all_records(&mut reader);

    assert_eq!(reader.byte_order_mark_length(), 3);
    assert_eq!(records[0].byte_range, 3..7);
    assert_eq!(records[0].fields, vec!["a", "b"]);
}

#[test]
fn foreign_byte_order_mark_is_ordinary_content() {
    let mut reader = open(b"\xFF\xFEa,b\n", dialect(WINDOWS_1252), 10, 64);
    let records = all_records(&mut reader);

    assert_eq!(reader.byte_order_mark_length(), 0);
    assert_eq!(records[0].fields, vec!["ÿþa", "b"]);
}

#[test]
fn windows_1252_fields_are_decoded() {
    assert_reads_as(
        b"Jos\xE9,M\xE1laga\r\nBego\xF1a,\"Le\xF3n, \x80 5\"\r\n",
        dialect(WINDOWS_1252),
        &[&["José", "Málaga"], &["Begoña", "León, € 5"]],
    );
}

#[test]
fn malformed_sequence_is_reported_on_its_record_only() {
    let mut reader = open(b"a,b\nc,\xFFd\ne,f\n", dialect(UTF_8), 10, 64);
    let records = all_records(&mut reader);

    assert_eq!(records[1].fields, vec!["c", "\u{FFFD}d"]);
    assert_eq!(
        records
            .iter()
            .map(|record| record.had_replacements)
            .collect::<Vec<_>>(),
        vec![false, true, false]
    );
}

#[test]
fn header_is_exposed_separately_and_excluded_from_pages_and_counts() {
    let with_header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };
    let bytes = b"\xEF\xBB\xBFid,name\n1,ana\n2,luis\n3,eva\n";

    let mut reader = open(bytes, with_header, 2, 64);

    let header = reader.header().unwrap().clone();
    assert_eq!(header.fields, vec!["id", "name"]);
    assert_eq!(header.byte_range, 3..11);

    let first = reader.read_page(0).unwrap();
    assert_eq!(first.first_record, 0);
    assert_eq!(
        field_rows(&first.records),
        vec![vec!["1", "ana"], vec!["2", "luis"]]
    );
    assert_eq!(first.records[0].byte_range, 11..17);

    let second = reader.read_page(1).unwrap();
    assert_eq!(second.first_record, 2);
    assert_eq!(field_rows(&second.records), vec![vec!["3", "eva"]]);

    assert_eq!(reader.record_count(), RecordCount::Total(3));

    assert_reads_as(
        bytes,
        with_header,
        &[&["1", "ana"], &["2", "luis"], &["3", "eva"]],
    );
}

#[test]
fn without_a_header_the_first_record_is_data() {
    let mut reader = open(b"id,name\n1,ana\n", dialect(UTF_8), 2, 64);

    assert!(reader.header().is_none());

    let page = reader.read_page(0).unwrap();
    assert_eq!(
        field_rows(&page.records),
        vec![vec!["id", "name"], vec!["1", "ana"]]
    );
    assert_eq!(reader.record_count(), RecordCount::Total(2));
}

/// Ten records of three bytes each: `r0\n` to `r9\n`.
fn ten_records() -> Vec<u8> {
    (0..10)
        .flat_map(|index| format!("r{index}\n").into_bytes())
        .collect()
}

#[test]
fn jumping_back_to_an_indexed_page_does_not_refetch_from_the_start() {
    let mut reader = open(&ten_records(), dialect(UTF_8), 2, 6);

    let far = reader.read_page(4).unwrap();
    assert_eq!(far.first_record, 8);
    assert_eq!(field_rows(&far.records), vec![vec!["r8"], vec!["r9"]]);

    reader.source().take_requests();

    let earlier = reader.read_page(1).unwrap();
    assert_eq!(field_rows(&earlier.records), vec![vec!["r2"], vec!["r3"]]);
    assert_eq!(reader.source().take_requests(), vec![6..12]);

    let later = reader.read_page(3).unwrap();
    assert_eq!(field_rows(&later.records), vec![vec!["r6"], vec!["r7"]]);
    assert_eq!(reader.source().take_requests(), vec![18..24]);
}

#[test]
fn jumping_forward_scans_from_the_last_indexed_page_only() {
    let mut reader = open(&ten_records(), dialect(UTF_8), 2, 6);

    reader.read_page(2).unwrap();
    reader.source().take_requests();

    reader.read_page(4).unwrap();

    // Page 3 starts at byte 18: nothing before it is fetched again.
    assert_eq!(reader.source().take_requests(), vec![18..24, 24..30]);
}

#[test]
fn total_is_unknown_until_a_scan_reaches_the_end() {
    let mut reader = open(&ten_records(), dialect(UTF_8), 2, 6);

    assert_eq!(reader.record_count(), RecordCount::IndexedSoFar(0));

    reader.read_page(0).unwrap();
    assert_eq!(reader.record_count(), RecordCount::IndexedSoFar(2));

    reader.read_page(2).unwrap();
    assert_eq!(reader.record_count(), RecordCount::IndexedSoFar(6));

    reader.read_page(0).unwrap();
    assert_eq!(reader.record_count(), RecordCount::IndexedSoFar(6));

    reader.read_page(4).unwrap();
    assert_eq!(reader.record_count(), RecordCount::Total(10));
}

#[test]
fn total_counts_a_partial_last_page() {
    let mut reader = open(&ten_records(), dialect(UTF_8), 4, 6);

    let last = reader.read_page(2).unwrap();

    assert_eq!(last.first_record, 8);
    assert_eq!(last.records.len(), 2);
    assert_eq!(reader.record_count(), RecordCount::Total(10));
}

#[test]
fn page_past_the_end_is_empty() {
    let mut reader = open(&ten_records(), dialect(UTF_8), 2, 6);

    let page = reader.read_page(7).unwrap();

    assert!(page.records.is_empty());
    assert_eq!(page.first_record, 14);
    assert_eq!(reader.record_count(), RecordCount::Total(10));

    // The total is known now, so asking again reads nothing.
    reader.source().take_requests();
    assert!(reader.read_page(5).unwrap().records.is_empty());
    assert_eq!(reader.source().take_requests(), vec![]);

    let last = reader.read_page(4).unwrap();
    assert_eq!(field_rows(&last.records), vec![vec!["r8"], vec!["r9"]]);
}

#[test]
fn empty_source_has_no_records() {
    let with_header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };

    for dialect in [dialect(UTF_8), with_header] {
        let mut reader = open(b"", dialect, 2, 64);

        assert!(reader.header().is_none());
        assert_eq!(reader.record_count(), RecordCount::Total(0));
        assert!(reader.read_page(0).unwrap().records.is_empty());
        assert_eq!(reader.byte_order_mark_length(), 0);
    }
}

#[test]
fn byte_order_mark_only_source_has_no_records() {
    let mut reader = open(b"\xEF\xBB\xBF", dialect(UTF_8), 2, 64);

    assert_eq!(reader.byte_order_mark_length(), 3);
    assert_eq!(reader.record_count(), RecordCount::Total(0));
    assert!(reader.read_page(0).unwrap().records.is_empty());
}

#[test]
fn header_only_source_has_a_header_and_no_records() {
    let with_header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };

    for bytes in [b"id,name\n".as_slice(), b"id,name".as_slice()] {
        let mut reader = open(bytes, with_header, 2, 64);

        assert_eq!(reader.header().unwrap().fields, vec!["id", "name"]);
        assert_eq!(reader.record_count(), RecordCount::Total(0));
        assert!(reader.read_page(0).unwrap().records.is_empty());
    }
}

#[test]
fn invalidating_from_a_page_rescans_it_and_everything_after() {
    let mut reader = open(&ten_records(), dialect(UTF_8), 2, 6);

    reader.read_page(4).unwrap();
    assert_eq!(reader.record_count(), RecordCount::Total(10));

    // `r2` in page 1 grows by five bytes, which moves every later page.
    reader.source_mut().bytes = b"r0\nr1\nlonger2\nr3\nr4\nr5\nr6\nr7\nr8\nr9\nr10\n".to_vec();
    reader.invalidate_from_page(1).unwrap();

    assert_eq!(reader.record_count(), RecordCount::IndexedSoFar(2));

    reader.source().take_requests();

    let third = reader.read_page(2).unwrap();
    assert_eq!(field_rows(&third.records), vec![vec!["r4"], vec!["r5"]]);
    assert_eq!(third.records[0].byte_range, 17..20);

    // Page 1 keeps its start, so the rescan begins there and not at byte 0.
    assert_eq!(reader.source().take_requests().first(), Some(&(6..12)));

    let last = reader.read_page(5).unwrap();
    assert_eq!(field_rows(&last.records), vec![vec!["r10"]]);
    assert_eq!(reader.record_count(), RecordCount::Total(11));
}

#[test]
fn invalidating_from_an_offset_keeps_the_pages_that_start_before_it() {
    let mut reader = open(&ten_records(), dialect(UTF_8), 2, 6);
    reader.read_page(4).unwrap();

    // Byte 15 is inside page 2, which starts at byte 12.
    reader.invalidate_from_offset(15).unwrap();
    assert_eq!(reader.record_count(), RecordCount::IndexedSoFar(4));

    reader.source().take_requests();
    reader.read_page(3).unwrap();

    assert_eq!(reader.source().take_requests().first(), Some(&(12..18)));
}

#[test]
fn invalidating_after_the_source_shrank_to_its_header_forgets_every_later_page() {
    let with_header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };
    let mut reader = open(b"id\n1\n2\n3\n4\n5\n", with_header, 2, 64);
    reader.read_page(2).unwrap();
    assert_eq!(reader.record_count(), RecordCount::Total(5));

    // Every record is gone, and the offset still points past page 1's start.
    reader.source_mut().bytes = b"id\n".to_vec();
    reader.invalidate_from_offset(9).unwrap();

    assert_eq!(reader.record_count(), RecordCount::Total(0));
    assert!(reader.read_page(1).unwrap().records.is_empty());
    assert_eq!(reader.record_count(), RecordCount::Total(0));
}

#[test]
fn invalidating_inside_the_header_reads_the_header_again() {
    let with_header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };
    let mut reader = open(b"id\n1\n2\n", with_header, 2, 64);
    reader.read_page(0).unwrap();

    reader.source_mut().bytes = b"identifier\n1\n2\n3\n".to_vec();
    reader.invalidate_from_offset(0).unwrap();

    assert_eq!(reader.header().unwrap().fields, vec!["identifier"]);
    assert_eq!(reader.header().unwrap().byte_range, 0..11);

    let records = all_records(&mut reader);
    assert_eq!(field_rows(&records), vec![vec!["1"], vec!["2"], vec!["3"]]);
    assert_eq!(reader.record_count(), RecordCount::Total(3));
}

#[test]
fn source_error_keeps_its_message() {
    let failing = |successful_reads| FailingSource {
        bytes: ten_records(),
        successful_reads: RefCell::new(successful_reads),
        message: "bucket unreachable: connection timed out",
    };

    let error = PagedReader::open(failing(0), dialect(UTF_8), options(2, 6))
        .err()
        .expect("opening must fail when the first read fails");

    assert!(matches!(error, ReadError::Source(_)));
    assert_eq!(
        error.to_string(),
        "bucket unreachable: connection timed out"
    );

    let mut reader = PagedReader::open(failing(1), dialect(UTF_8), options(2, 6)).unwrap();
    assert_eq!(reader.read_page(0).unwrap().records.len(), 2);

    let error = reader.read_page(1).unwrap_err();
    assert_eq!(
        error.to_string(),
        "bucket unreachable: connection timed out"
    );
}

#[test]
fn source_error_exposes_the_callers_error() {
    let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");

    let error = SourceError::from(io_error);
    assert_eq!(error.to_string(), "access denied");

    let inner = error.into_inner();
    let io_error = inner.downcast_ref::<std::io::Error>().unwrap();
    assert_eq!(io_error.kind(), std::io::ErrorKind::PermissionDenied);
}

/// A source that claims more bytes than it returns.
struct TruncatingSource;

impl ByteSource for TruncatingSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(100)
    }

    fn read_range(&self, _range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        Ok(b"ab".to_vec())
    }
}

#[test]
fn wrong_read_length_is_an_error_and_not_an_endless_loop() {
    let error = PagedReader::open(TruncatingSource, dialect(UTF_8), options(2, 8))
        .err()
        .expect("a source that returns fewer bytes than its length must be refused");

    assert!(matches!(
        error,
        ReadError::UnexpectedReadLength {
            start: 0,
            end: 8,
            received: 2
        }
    ));
}

#[test]
fn dialect_that_cannot_be_scanned_by_byte_is_refused() {
    let refused = |dialect: Dialect| {
        matches!(
            PagedReader::open(MemorySource::new(b"a\n".to_vec()), dialect, options(2, 8)),
            Err(ReadError::UnsupportedDialect { .. })
        )
    };

    let pipe = |encoding| Dialect {
        delimiter: b'|',
        ..dialect(encoding)
    };

    // 0x7C is a trail byte in these encodings.
    assert!(refused(pipe(SHIFT_JIS)));
    assert!(refused(pipe(GBK)));
    assert!(refused(pipe(BIG5)));

    assert!(!refused(dialect(SHIFT_JIS)));
    assert!(!refused(pipe(EUC_JP)));
    assert!(!refused(pipe(UTF_8)));
    assert!(!refused(pipe(WINDOWS_1252)));

    assert!(refused(dialect(ISO_2022_JP)));

    let high_delimiter = |encoding| Dialect {
        delimiter: 0xA7,
        ..dialect(encoding)
    };

    assert!(refused(high_delimiter(UTF_8)));
    assert!(refused(high_delimiter(EUC_JP)));
    assert!(!refused(high_delimiter(WINDOWS_1252)));
    assert!(!refused(high_delimiter(UTF_16LE)));
}

#[test]
fn shift_jis_fields_are_decoded() {
    // U+8868 is 0x95 0x5C and U+30BD is 0x83 0x5C in Shift_JIS.
    let (bytes, _, had_unmappable) = SHIFT_JIS.encode("表,ソ\n\"予定,表\",x\n");
    assert!(!had_unmappable);

    assert_reads_as(
        &bytes,
        dialect(SHIFT_JIS),
        &[&["表", "ソ"], &["予定,表", "x"]],
    );
}

/// Pins the rule the reader relies on for multi-byte legacy encodings: a byte
/// it accepts as a delimiter or quote never occurs inside a multi-byte
/// character.
#[test]
fn accepted_special_bytes_never_occur_inside_a_multibyte_character() {
    let accepted: Vec<u8> = (0..=u8::MAX)
        .filter(|byte| never_collides_with_a_trail_byte(*byte))
        .collect();

    assert!(accepted.contains(&b','));
    assert!(accepted.contains(&b';'));
    assert!(accepted.contains(&b'\t'));
    assert!(accepted.contains(&b'"'));
    assert!(accepted.contains(&b'\n'));
    assert!(accepted.contains(&b'\r'));
    assert!(!accepted.contains(&b'|'));

    let mut buffer = [0u8; 4];

    for encoding in [SHIFT_JIS, GBK, GB18030, BIG5, EUC_KR] {
        for character in ('\u{80}'..=char::MAX).filter(|character| *character != '\u{FFFD}') {
            let (bytes, _, had_unmappable) = encoding.encode(character.encode_utf8(&mut buffer));

            if had_unmappable {
                continue;
            }

            assert!(
                !bytes
                    .iter()
                    .any(|byte| never_collides_with_a_trail_byte(*byte)),
                "{} encodes {character:?} as {bytes:?}",
                encoding.name()
            );
        }
    }
}

#[test]
fn memory_source_clamps_a_read_to_its_length() {
    let source = MemorySource::new(b"0123456789".to_vec());

    assert_eq!(source.byte_length().unwrap(), 10);
    assert_eq!(source.read_range(2..5).unwrap(), b"234");
    assert_eq!(source.read_range(8..50).unwrap(), b"89");
    assert_eq!(source.read_range(10..12).unwrap(), b"");
    assert_eq!(source.read_range(40..50).unwrap(), b"");
    assert_eq!(source.bytes(), b"0123456789");
}

#[test]
fn file_source_reads_positioned_ranges() {
    let path = std::env::temp_dir().join(format!(
        "dbflux_delimited_file_source_{}.csv",
        std::process::id()
    ));
    std::fs::write(&path, b"id,name\n1,ana\n2,luis\n").unwrap();

    let source = FileSource::open(&path).unwrap();

    assert_eq!(source.byte_length().unwrap(), 21);
    assert_eq!(source.read_range(8..14).unwrap(), b"1,ana\n");
    assert_eq!(source.read_range(14..500).unwrap(), b"2,luis\n");
    assert_eq!(source.read_range(21..30).unwrap(), b"");

    let with_header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };
    let mut reader = PagedReader::open(source, with_header, options(1, 4)).unwrap();
    let page = reader.read_page(1).unwrap();

    std::fs::remove_file(&path).unwrap();

    assert_eq!(page.records[0].fields, vec!["2", "luis"]);
    assert_eq!(page.records[0].byte_range, 14..21);
}

#[test]
fn file_source_open_error_keeps_the_io_message() {
    let missing = std::env::temp_dir().join("dbflux_delimited_missing/none.csv");

    let error = FileSource::open(&missing).expect_err("opening a missing file must fail");

    let expected = std::fs::File::open(&missing).unwrap_err().to_string();
    assert_eq!(error.to_string(), expected);
}

#[test]
fn invalidating_from_offset_zero_reads_the_mark_and_header_of_a_source_that_was_empty() {
    let with_header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };
    let mut reader = open(b"", with_header, 2, 64);

    reader.source_mut().bytes = b"\xEF\xBB\xBFh1,h2\na,1\nb,2\n".to_vec();
    reader.invalidate_from_offset(0).unwrap();

    assert_eq!(reader.byte_order_mark_length(), 3);
    assert_eq!(reader.header().unwrap().fields, vec!["h1", "h2"]);
    assert_eq!(reader.header().unwrap().byte_range, 3..9);

    let records = all_records(&mut reader);
    assert_eq!(field_rows(&records), vec![vec!["a", "1"], vec!["b", "2"]]);
}

#[test]
fn invalidating_from_offset_zero_reads_a_mark_added_to_headerless_data() {
    let mut reader = open(b"a,1\n", dialect(UTF_8), 2, 64);
    reader.read_page(0).unwrap();

    reader.source_mut().bytes = b"\xEF\xBB\xBFa,1\n".to_vec();
    reader.invalidate_from_offset(0).unwrap();

    assert_eq!(reader.byte_order_mark_length(), 3);

    let page = reader.read_page(0).unwrap();
    assert_eq!(page.records[0].fields, vec!["a", "1"]);
    assert_eq!(page.records[0].byte_range, 3..7);
}

#[test]
fn invalidating_from_page_zero_reads_the_mark_and_header_again() {
    let with_header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };
    let mut reader = open(b"\xEF\xBB\xBFid\n1\n2\n", with_header, 2, 64);
    reader.read_page(0).unwrap();

    reader.source_mut().bytes = Vec::new();
    reader.invalidate_from_page(0).unwrap();

    assert!(reader.header().is_none());
    assert_eq!(reader.byte_order_mark_length(), 0);
    assert_eq!(reader.record_count(), RecordCount::Total(0));
    assert!(reader.read_page(0).unwrap().records.is_empty());
}

#[test]
fn line_break_as_delimiter_or_quote_is_refused() {
    let open_with = |dialect: Dialect| {
        PagedReader::open(MemorySource::new(b"a\n".to_vec()), dialect, options(2, 8))
            .map(|_reader| ())
    };

    for byte in [b'\n', b'\r'] {
        for encoding in [UTF_8, UTF_16LE, WINDOWS_1252] {
            let as_delimiter = Dialect {
                delimiter: byte,
                ..dialect(encoding)
            };
            let as_quote = Dialect {
                quote: Some(byte),
                ..dialect(encoding)
            };

            for degenerate in [as_delimiter, as_quote] {
                let error = open_with(degenerate).unwrap_err();

                assert!(
                    matches!(error, ReadError::LineBreakInDialect { byte: refused } if refused == byte),
                    "{error:?}"
                );
            }
        }
    }
}

#[test]
fn quote_equal_to_the_delimiter_is_refused() {
    let same = Dialect {
        delimiter: b';',
        quote: Some(b';'),
        ..dialect(UTF_8)
    };

    let error = PagedReader::open(MemorySource::new(b"a\n".to_vec()), same, options(2, 8))
        .map(|_reader| ())
        .unwrap_err();

    assert!(matches!(
        error,
        ReadError::QuoteEqualsDelimiter { byte: b';' }
    ));
    assert_eq!(
        error.to_string(),
        "the quote and the delimiter are both ';': choose a different character for one of them"
    );
}

// -- The bytes of a page ---------------------------------------------------------

/// The bytes of the leading `records` whose bytes together fit in `budget`,
/// cut from `source`.
fn leading_bytes(source: &[u8], records: &[Record], budget: usize) -> Vec<u8> {
    let mut kept = Vec::new();

    for record in records {
        let bytes = &source[record.byte_range.start as usize..record.byte_range.end as usize];

        if kept.len() + bytes.len() > budget {
            break;
        }

        kept.extend_from_slice(bytes);
    }

    kept
}

#[test]
fn a_page_read_with_its_bytes_returns_them_and_reads_nothing_more() {
    let mixed: &[u8] = b"h1,h2\r\na,\"x\ny\"\n3,4\r5,\"q\"\"r\"\r\n\n7,8";
    let mut with_mark = UTF_8_MARK_BYTES.to_vec();
    with_mark.extend_from_slice(b"a,b\n\"one\r\ntwo\",3\n4,5\n");
    let mut utf16 = vec![0xFF, 0xFE];
    utf16.extend(utf16le("a,b\r\n\"x\ny\",2\n3,4"));

    let header = |encoding| Dialect {
        has_header: true,
        ..dialect(encoding)
    };

    let cases: Vec<(&[u8], Dialect)> = vec![
        (mixed, header(UTF_8)),
        (mixed, dialect(UTF_8)),
        (&with_mark, header(UTF_8)),
        (&utf16, header(UTF_16LE)),
    ];

    for (bytes, dialect) in cases {
        for window in 1..=bytes.len() as u64 + 1 {
            for page_size in [1, 2, 3] {
                for budget in [0, 1, 9, 17, usize::MAX] {
                    let mut plain = open(bytes, dialect, page_size, window);
                    let mut kept = open(bytes, dialect, page_size, window);

                    let header_range = plain.header().map(|record| {
                        record.byte_range.start as usize..record.byte_range.end as usize
                    });
                    assert_eq!(
                        kept.header_bytes(),
                        header_range.map(|range| &bytes[range]),
                        "window {window}"
                    );

                    assert_eq!(
                        plain.source().take_requests(),
                        kept.source().take_requests()
                    );

                    for page_index in 0..bytes.len() + 2 {
                        let page = plain.read_page(page_index).unwrap();
                        let (kept_page, page_bytes) =
                            kept.read_page_with_bytes(page_index, budget).unwrap();

                        assert_eq!(kept_page, page, "window {window} page {page_index}");
                        assert_eq!(
                            page_bytes,
                            leading_bytes(bytes, &page.records, budget),
                            "window {window} page size {page_size} budget {budget} page {page_index}"
                        );
                        assert_eq!(
                            plain.source().take_requests(),
                            kept.source().take_requests(),
                            "the bytes cost no read: window {window} page {page_index}"
                        );

                        if page.records.is_empty() {
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn a_page_read_with_its_bytes_out_of_order_returns_the_same_bytes() {
    let bytes = b"0\n11\n222\n3333\n44444\n";
    let mut reader = open(bytes, dialect(UTF_8), 2, 3);

    let (third, third_bytes) = reader.read_page_with_bytes(2, usize::MAX).unwrap();
    let (first, first_bytes) = reader.read_page_with_bytes(0, usize::MAX).unwrap();

    assert_eq!(
        third_bytes,
        leading_bytes(bytes, &third.records, usize::MAX)
    );
    assert_eq!(first_bytes, b"0\n11\n");
    assert_eq!(first.records.len(), 2);
}

#[test]
fn the_header_bytes_follow_an_invalidation() {
    let header = Dialect {
        has_header: true,
        ..dialect(UTF_8)
    };
    let mut reader = PagedReader::open(
        MemorySource::new(b"a,b\n1,2\n".to_vec()),
        header,
        options(10, 4),
    )
    .unwrap();

    assert_eq!(reader.header_bytes(), Some(&b"a,b\n"[..]));

    *reader.source_mut() = MemorySource::new(b"name,city\r\n1,2\n".to_vec());
    reader.invalidate_from_offset(0).unwrap();

    assert_eq!(reader.header_bytes(), Some(&b"name,city\r\n"[..]));

    let headerless = PagedReader::open(
        MemorySource::new(b"a,b\n".to_vec()),
        dialect(UTF_8),
        options(10, 4),
    )
    .unwrap();

    assert_eq!(headerless.header_bytes(), None);
}

const UTF_8_MARK_BYTES: &[u8] = b"\xEF\xBB\xBF";
