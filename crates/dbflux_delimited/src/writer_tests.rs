// A deletion is a byte range, so a single deletion is a `Vec` of one range.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::single_range_in_vec_init
)]

use std::cell::RefCell;
use std::io::Write;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;

use encoding_rs::{Encoding, UTF_8, UTF_16BE, UTF_16LE, WINDOWS_1252};

use super::{
    AppendedColumn, ByteSource, Dialect, EditLocation, EditSet, InsertPosition, Insertion,
    MemorySource, PagedReader, ReadError, ReaderOptions, Record, Replacement, SourceError,
    WriteError, write_edited,
};

/// Four records with four different endings: CRLF, a quoted line feed before
/// an LF terminator, a bare CR, and a doubled quote before CRLF.
const MIXED: &[u8] = b"a,b\r\n\"x\ny\",2\n3,4\r5,\"q\"\"r\"\r\n";

const UTF_8_MARK: &[u8] = b"\xEF\xBB\xBF";

fn dialect(encoding: &'static Encoding) -> Dialect {
    Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: false,
        encoding,
    }
}

fn with_header(dialect: Dialect) -> Dialect {
    Dialect {
        has_header: true,
        ..dialect
    }
}

fn without_quote(dialect: Dialect) -> Dialect {
    Dialect {
        quote: None,
        ..dialect
    }
}

/// Saves `bytes` with `edits`, whose source length is set here to the length
/// of `bytes` whatever the tests built it with.
fn try_save(
    bytes: &[u8],
    dialect: Dialect,
    edits: &EditSet,
    window_size: u64,
) -> (Result<(), WriteError>, Vec<u8>) {
    let source = MemorySource::new(bytes.to_vec());
    let mut output = Vec::new();

    let edits = EditSet {
        source_length: bytes.len() as u64,
        ..edits.clone()
    };

    let result = write_edited(
        &source,
        &dialect,
        &edits,
        NonZeroU64::new(window_size).unwrap(),
        &mut output,
    );

    (result, output)
}

fn save(bytes: &[u8], dialect: Dialect, edits: &EditSet) -> Vec<u8> {
    let (result, output) = try_save(bytes, dialect, edits, 5);
    result.unwrap();

    output
}

fn save_error(bytes: &[u8], dialect: Dialect, edits: &EditSet) -> WriteError {
    try_save(bytes, dialect, edits, 5).0.unwrap_err()
}

/// Saves with every window size from one byte to past the end of the source
/// and checks that each output equals `expected`.
fn assert_saves_as(bytes: &[u8], dialect: Dialect, edits: &EditSet, expected: &[u8]) {
    for window_size in 1..=bytes.len() as u64 + 1 {
        let (result, output) = try_save(bytes, dialect, edits, window_size);
        result.unwrap();

        assert_eq!(output, expected, "window size {window_size}");
    }
}

fn read(bytes: &[u8], dialect: Dialect) -> (Option<Record>, Vec<Record>) {
    let options = ReaderOptions {
        page_size: NonZeroUsize::new(3).unwrap(),
        window_size: NonZeroU64::new(4).unwrap(),
    };

    let mut reader =
        PagedReader::open(MemorySource::new(bytes.to_vec()), dialect, options).unwrap();
    let mut records = Vec::new();

    for page in 0.. {
        let page = reader.read_page(page).unwrap();

        if page.records.is_empty() {
            break;
        }

        records.extend(page.records);
    }

    (reader.header().cloned(), records)
}

fn ranges(bytes: &[u8], dialect: Dialect) -> Vec<Range<u64>> {
    read(bytes, dialect)
        .1
        .into_iter()
        .map(|record| record.byte_range)
        .collect()
}

fn rows(bytes: &[u8], dialect: Dialect) -> Vec<Vec<String>> {
    read(bytes, dialect)
        .1
        .into_iter()
        .map(|record| record.fields)
        .collect()
}

fn fields(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn replace(byte_range: Range<u64>, values: &[&str]) -> Replacement {
    Replacement {
        byte_range,
        fields: fields(values),
    }
}

fn insert(position: InsertPosition, values: &[&str]) -> Insertion {
    Insertion {
        position,
        fields: fields(values),
    }
}

fn insert_at_end(values: &[&str]) -> EditSet {
    EditSet {
        insertions: vec![insert(InsertPosition::End, values)],
        ..EditSet::new(0)
    }
}

fn column(header: &str, default_value: &str, values: &[(Range<u64>, &str)]) -> AppendedColumn {
    AppendedColumn {
        header: header.to_owned(),
        default_value: default_value.to_owned(),
        values: values
            .iter()
            .map(|(range, value)| (range.clone(), (*value).to_owned()))
            .collect(),
    }
}

fn concat(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

fn utf16le(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn utf16be(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_be_bytes).collect()
}

/// What a [`FlakySource`] does once its good reads are used up.
enum Failure {
    Error(&'static str),
    OneByteShort,
}

/// A source that serves `good_reads` reads and then fails.
struct FlakySource {
    bytes: Vec<u8>,
    good_reads: RefCell<usize>,
    failure: Failure,
}

impl ByteSource for FlakySource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(self.bytes.len() as u64)
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let end = (range.end as usize).min(self.bytes.len());
        let start = (range.start as usize).min(end);

        let mut good_reads = self.good_reads.borrow_mut();

        if *good_reads > 0 {
            *good_reads -= 1;

            return Ok(self.bytes[start..end].to_vec());
        }

        match self.failure {
            Failure::Error(message) => Err(SourceError::new(message)),
            Failure::OneByteShort => Ok(self.bytes[start..end.saturating_sub(1)].to_vec()),
        }
    }
}

/// An in-memory source that remembers every range it was asked for.
struct RecordingSource {
    bytes: Vec<u8>,
    requests: RefCell<Vec<Range<u64>>>,
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

/// A sink that accepts `capacity` bytes and then fails.
struct FullSink {
    capacity: usize,
}

impl Write for FullSink {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.capacity == 0 {
            return Err(std::io::Error::other("the disk is full"));
        }

        let written = buffer.len().min(self.capacity);
        self.capacity -= written;

        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn an_empty_edit_set_writes_the_source_byte_for_byte() {
    let utf16_with_mark = concat(&[b"\xFF\xFE", &utf16le("a,b\r\n\"x\ny\",2\nc")]);

    let sources: [(&[u8], &'static Encoding); 9] = [
        (b"a,b\nc,d\n", UTF_8),
        (b"a,b\r\nc,d\r\n", UTF_8),
        (b"a,b\rc,d\r", UTF_8),
        (MIXED, UTF_8),
        (b"a,b\nc,d", UTF_8),
        (b"a,b\r\n\"x\ny\",2\n3,4\r5", UTF_8),
        (b"\xEF\xBB\xBFa,b\r\nc,d\n", UTF_8),
        (b"caf\xE9,b\r\n\n\nc,d", WINDOWS_1252),
        (&utf16_with_mark, UTF_16LE),
    ];

    for (bytes, encoding) in sources {
        assert!(EditSet::new(0).is_empty());

        assert_saves_as(bytes, dialect(encoding), &EditSet::new(0), bytes);
        assert_saves_as(
            bytes,
            with_header(dialect(encoding)),
            &EditSet::new(0),
            bytes,
        );
    }
}

#[test]
fn a_replaced_record_leaves_every_other_byte_and_keeps_its_terminator() {
    let records = ranges(MIXED, dialect(UTF_8));

    let edits = EditSet {
        replacements: vec![replace(records[2].clone(), &["30", "40"])],
        ..EditSet::new(0)
    };

    let expected = concat(&[&MIXED[..13], b"30,40\r", &MIXED[17..]]);

    assert_saves_as(MIXED, dialect(UTF_8), &edits, &expected);
}

#[test]
fn an_edited_record_is_not_read_again_for_its_terminator() {
    let records = ranges(MIXED, dialect(UTF_8));

    let source = RecordingSource {
        bytes: MIXED.to_vec(),
        requests: RefCell::new(Vec::new()),
    };

    let edits = EditSet {
        replacements: vec![replace(records[2].clone(), &["30", "40"])],
        insertions: vec![insert(
            InsertPosition::Before(records[3].clone()),
            &["n", "m"],
        )],
        ..EditSet::new(MIXED.len() as u64)
    };

    let mut output = Vec::new();

    write_edited(
        &source,
        &dialect(UTF_8),
        &edits,
        NonZeroU64::new(64).unwrap(),
        &mut output,
    )
    .unwrap();

    let expected = concat(&[&MIXED[..13], b"30,40\r", b"n,m\r\n", &MIXED[17..]]);
    assert_eq!(output, expected);

    // The terminator of an edited record comes from the read that checks its
    // boundaries, so no read starts in the middle of the record to fetch its
    // last code units again.
    for record in [&records[2], &records[3]] {
        let tail_reads: Vec<_> = source
            .requests
            .borrow()
            .iter()
            .filter(|request| request.start > record.start && request.end <= record.end)
            .cloned()
            .collect();

        assert!(tail_reads.is_empty(), "{record:?}: {tail_reads:?}");
    }
}

#[test]
fn a_replacement_can_make_its_record_longer_or_shorter() {
    let records = ranges(MIXED, dialect(UTF_8));

    let longer = EditSet {
        replacements: vec![replace(records[1].clone(), &["a much longer value", "2"])],
        ..EditSet::new(0)
    };

    let shorter = EditSet {
        replacements: vec![replace(records[1].clone(), &["", "2"])],
        ..EditSet::new(0)
    };

    assert_eq!(
        save(MIXED, dialect(UTF_8), &longer),
        concat(&[&MIXED[..5], b"a much longer value,2\n", &MIXED[13..]])
    );

    assert_eq!(
        save(MIXED, dialect(UTF_8), &shorter),
        concat(&[&MIXED[..5], b",2\n", &MIXED[13..]])
    );
}

#[test]
fn a_replaced_final_record_without_a_terminator_stays_without_one() {
    let edits = EditSet {
        replacements: vec![replace(2..3, &["z"])],
        ..EditSet::new(0)
    };

    assert_eq!(save(b"a\nb", dialect(UTF_8), &edits), b"a\nz");
}

#[test]
fn replacing_the_header_renames_a_column() {
    let bytes = b"id,name\r\n1,x\n";
    let dialect = with_header(dialect(UTF_8));

    let header = read(bytes, dialect).0.unwrap();

    let edits = EditSet {
        replacements: vec![replace(header.byte_range, &["id", "label"])],
        ..EditSet::new(0)
    };

    let output = save(bytes, dialect, &edits);

    assert_eq!(output, b"id,label\r\n1,x\n");
    assert_eq!(
        read(&output, dialect).0.unwrap().fields,
        fields(&["id", "label"])
    );
}

#[test]
fn deleting_a_record_removes_exactly_its_range() {
    let records = ranges(MIXED, dialect(UTF_8));

    let delete = |index: usize| EditSet {
        deletions: vec![records[index].clone()],
        ..EditSet::new(0)
    };

    assert_saves_as(MIXED, dialect(UTF_8), &delete(0), &MIXED[5..]);

    assert_saves_as(
        MIXED,
        dialect(UTF_8),
        &delete(1),
        &concat(&[&MIXED[..5], &MIXED[13..]]),
    );

    assert_saves_as(MIXED, dialect(UTF_8), &delete(3), &MIXED[..17]);
}

#[test]
fn deleting_a_final_record_without_a_terminator_keeps_the_one_before_it() {
    let edits = EditSet {
        deletions: vec![2..3],
        ..EditSet::new(0)
    };

    assert_eq!(save(b"a\nb", dialect(UTF_8), &edits), b"a\n");
}

#[test]
fn an_inserted_record_takes_the_terminator_of_the_record_it_is_placed_before() {
    let records = ranges(MIXED, dialect(UTF_8));

    let before = |index: usize| EditSet {
        insertions: vec![insert(
            InsertPosition::Before(records[index].clone()),
            &["n", "1"],
        )],
        ..EditSet::new(0)
    };

    assert_saves_as(
        MIXED,
        dialect(UTF_8),
        &before(0),
        &concat(&[b"n,1\r\n", MIXED]),
    );

    assert_saves_as(
        MIXED,
        dialect(UTF_8),
        &before(2),
        &concat(&[&MIXED[..13], b"n,1\r", &MIXED[13..]]),
    );
}

#[test]
fn a_record_inserted_at_the_end_takes_the_terminator_of_the_last_record() {
    assert_saves_as(
        MIXED,
        dialect(UTF_8),
        &insert_at_end(&["n", "1"]),
        &concat(&[MIXED, b"n,1\r\n"]),
    );

    assert_saves_as(
        b"a,b\rc,d\n",
        dialect(UTF_8),
        &insert_at_end(&["n", "1"]),
        b"a,b\rc,d\nn,1\n",
    );
}

#[test]
fn inserting_after_a_final_record_without_a_terminator_gives_that_record_one() {
    assert_saves_as(
        b"a,b\r\nc,d",
        dialect(UTF_8),
        &insert_at_end(&["n", "1"]),
        b"a,b\r\nc,d\r\nn,1\r\n",
    );

    assert_saves_as(
        b"a,b",
        dialect(UTF_8),
        &insert_at_end(&["n", "1"]),
        b"a,b\nn,1\n",
    );

    let replaced_and_inserted = EditSet {
        replacements: vec![replace(5..8, &["x", "y"])],
        insertions: vec![insert(InsertPosition::End, &["n", "1"])],
        ..EditSet::new(0)
    };

    assert_saves_as(
        b"a,b\r\nc,d",
        dialect(UTF_8),
        &replaced_and_inserted,
        b"a,b\r\nx,y\r\nn,1\r\n",
    );
}

#[test]
fn inserting_after_a_deleted_final_record_adds_no_terminator_for_it() {
    let edits = EditSet {
        deletions: vec![5..8],
        insertions: vec![insert(InsertPosition::End, &["n", "1"])],
        ..EditSet::new(0)
    };

    assert_saves_as(b"a,b\r\nc,d", dialect(UTF_8), &edits, b"a,b\r\nn,1\r\n");
}

#[test]
fn inserting_before_a_final_record_without_a_terminator_uses_the_first_terminator() {
    let edits = EditSet {
        insertions: vec![insert(InsertPosition::Before(5..8), &["n", "1"])],
        ..EditSet::new(0)
    };

    assert_saves_as(b"a,b\r\nc,d", dialect(UTF_8), &edits, b"a,b\r\nn,1\r\nc,d");
}

#[test]
fn inserting_into_a_file_without_records_uses_a_line_feed() {
    let edits = insert_at_end(&["n", "1"]);

    assert_eq!(save(b"", dialect(UTF_8), &edits), b"n,1\n");

    assert_eq!(
        save(UTF_8_MARK, dialect(UTF_8), &edits),
        b"\xEF\xBB\xBFn,1\n"
    );

    assert_eq!(
        save(b"", dialect(UTF_16BE), &edits),
        [0, b'n', 0, b',', 0, b'1', 0, b'\n']
    );
}

#[test]
fn inserting_after_only_a_header_uses_the_header_terminator() {
    assert_saves_as(
        b"h1,h2\r\n",
        with_header(dialect(UTF_8)),
        &insert_at_end(&["n", "1"]),
        b"h1,h2\r\nn,1\r\n",
    );
}

#[test]
fn several_edits_in_any_order_apply_in_one_save() {
    let records = ranges(MIXED, dialect(UTF_8));

    let edits = EditSet {
        source_length: 0,
        insertions: vec![
            insert(InsertPosition::End, &["e", "1"]),
            insert(InsertPosition::Before(records[2].clone()), &["i", "1"]),
            insert(InsertPosition::End, &["e", "2"]),
            insert(InsertPosition::Before(records[2].clone()), &["i", "2"]),
        ],
        replacements: vec![
            replace(records[3].clone(), &["5", "new"]),
            replace(records[1].clone(), &["x y", "2"]),
        ],
        deletions: vec![records[0].clone()],
        appended_columns: Vec::new(),
    };

    let expected = b"x y,2\ni,1\ri,2\r3,4\r5,new\r\ne,1\r\ne,2\r\n";

    assert_saves_as(MIXED, dialect(UTF_8), &edits, expected);

    assert_eq!(
        rows(expected, dialect(UTF_8)),
        [
            fields(&["x y", "2"]),
            fields(&["i", "1"]),
            fields(&["i", "2"]),
            fields(&["3", "4"]),
            fields(&["5", "new"]),
            fields(&["e", "1"]),
            fields(&["e", "2"]),
        ]
    );
}

#[test]
fn appending_a_column_inserts_a_field_before_each_terminator() {
    let edits = EditSet {
        appended_columns: vec![column("ignored without a header", "", &[])],
        ..EditSet::new(0)
    };

    let expected = b"a,b,\r\n\"x\ny\",2,\n3,4,\r5,\"q\"\"r\",\r\n";

    assert_saves_as(MIXED, dialect(UTF_8), &edits, expected);

    assert_eq!(
        rows(expected, dialect(UTF_8)),
        [
            fields(&["a", "b", ""]),
            fields(&["x\ny", "2", ""]),
            fields(&["3", "4", ""]),
            fields(&["5", "q\"r", ""]),
        ]
    );
}

#[test]
fn appending_a_column_names_it_in_the_header_and_takes_values_per_record() {
    let bytes = b"\xEF\xBB\xBFid,name\n1,x\r\n2,\"y\nz\"";
    let dialect = with_header(dialect(UTF_8));

    let records = ranges(bytes, dialect);

    let edits = EditSet {
        appended_columns: vec![column("extra", "0", &[(records[1].clone(), "9,5")])],
        ..EditSet::new(0)
    };

    let expected = b"\xEF\xBB\xBFid,name,extra\n1,x,0\r\n2,\"y\nz\",\"9,5\"";

    assert_saves_as(bytes, dialect, &edits, expected);

    let (header, records) = read(expected, dialect);

    assert_eq!(header.unwrap().fields, fields(&["id", "name", "extra"]));
    assert_eq!(records[0].fields, fields(&["1", "x", "0"]));
    assert_eq!(records[1].fields, fields(&["2", "y\nz", "9,5"]));
}

#[test]
fn a_header_with_a_value_of_its_own_keeps_the_names_of_the_other_columns() {
    let bytes = b"id\n1\n";
    let dialect = with_header(dialect(UTF_8));

    let edits = EditSet {
        appended_columns: vec![
            column("first", "a", &[(0..3, "custom")]),
            column("second", "b", &[]),
        ],
        ..EditSet::new(0)
    };

    assert_saves_as(bytes, dialect, &edits, b"id,custom,second\n1,a,b\n");
}

#[test]
fn appended_columns_combine_with_other_edits_and_skip_rendered_records() {
    let records = ranges(MIXED, dialect(UTF_8));

    let edits = EditSet {
        source_length: 0,
        appended_columns: vec![
            column("", "p", &[]),
            column("", "q", &[(records[1].clone(), "z")]),
        ],
        replacements: vec![replace(records[2].clone(), &["3", "4", "P", "Q"])],
        deletions: vec![records[0].clone()],
        insertions: vec![
            insert(InsertPosition::End, &["n", "n", "n", "n"]),
            insert(InsertPosition::Before(records[3].clone()), &["m"]),
        ],
    };

    let expected = b"\"x\ny\",2,p,z\n3,4,P,Q\rm\r\n5,\"q\"\"r\",p,q\r\nn,n,n,n\r\n";

    assert_saves_as(MIXED, dialect(UTF_8), &edits, expected);
}

#[test]
fn appending_a_column_to_an_empty_line_makes_it_two_fields() {
    let edits = EditSet {
        appended_columns: vec![column("", "v", &[])],
        ..EditSet::new(0)
    };

    assert_saves_as(b"a\n\nb", dialect(UTF_8), &edits, b"a,v\n,v\nb,v");
}

#[test]
fn appending_a_column_to_utf_16_writes_whole_code_units() {
    let bytes = concat(&[b"\xFF\xFE", &utf16le("a\r\n\"b\nc\"\nd")]);

    let edits = EditSet {
        appended_columns: vec![column("", "x", &[])],
        ..EditSet::new(0)
    };

    let expected = concat(&[b"\xFF\xFE", &utf16le("a,x\r\n\"b\nc\",x\nd,x")]);

    assert_saves_as(&bytes, dialect(UTF_16LE), &edits, &expected);
}

#[test]
fn appending_a_column_refuses_a_record_with_an_unclosed_quote() {
    let edits = EditSet {
        appended_columns: vec![column("", "v", &[])],
        ..EditSet::new(0)
    };

    let error = save_error(b"a\n\"open\n", dialect(UTF_8), &edits);

    assert!(
        matches!(&error, WriteError::UnclosedQuote { byte_range } if *byte_range == (2..8)),
        "{error:?}"
    );
    assert!(error.to_string().contains("bytes 2..8"), "{error}");
}

#[test]
fn a_field_is_quoted_only_when_the_dialect_needs_it() {
    let edits = insert_at_end(&[
        "plain",
        "a,b",
        "say \"hi\"",
        "line\nbreak",
        "cr\rhere",
        " lead",
        "trail ",
        "",
        "in ner",
    ]);

    assert_eq!(
        save(b"", dialect(UTF_8), &edits),
        b"plain,\"a,b\",\"say \"\"hi\"\"\",\"line\nbreak\",\"cr\rhere\",\" lead\",\"trail \",,in ner\n"
    );
}

#[test]
fn a_record_of_one_empty_field_is_written_as_a_pair_of_quotes() {
    assert_eq!(save(b"", dialect(UTF_8), &insert_at_end(&[""])), b"\"\"\n");
    assert_eq!(save(b"", dialect(UTF_8), &insert_at_end(&[])), b"\"\"\n");

    let replace_final = EditSet {
        replacements: vec![replace(2..3, &[""])],
        ..EditSet::new(0)
    };

    let output = save(b"a\nb", dialect(UTF_8), &replace_final);

    assert_eq!(output, b"a\n\"\"");
    assert_eq!(
        rows(&output, dialect(UTF_8)),
        [fields(&["a"]), fields(&[""])]
    );
}

#[test]
fn without_a_quote_a_field_that_needs_one_is_refused() {
    let dialect = without_quote(dialect(UTF_8));

    for (values, field) in [
        (&["ok", "a,b"][..], 1),
        (&["line\nbreak", "ok"][..], 0),
        (&["ok", "ok", "cr\rhere"][..], 2),
        (&[""][..], 0),
    ] {
        let (result, output) = try_save(b"x,y\n", dialect, &insert_at_end(values), 5);
        let error = result.unwrap_err();

        assert!(
            matches!(
                &error,
                WriteError::UnquotableField { record: EditLocation::Inserted(0), field: refused }
                    if *refused == field
            ),
            "{values:?}: {error:?}"
        );
        assert!(error.to_string().contains("no quote character"), "{error}");
        assert!(output.is_empty(), "{values:?}");
    }
}

#[test]
fn without_a_quote_padding_and_quote_characters_are_written_as_they_are() {
    let dialect = without_quote(dialect(UTF_8));

    let output = save(b"x,y\n", dialect, &insert_at_end(&[" lead", "say \"hi\" "]));

    assert_eq!(output, b"x,y\n lead,say \"hi\" \n");
    assert_eq!(rows(&output, dialect)[1], fields(&[" lead", "say \"hi\" "]));
}

#[test]
fn rendered_records_read_back_as_the_same_fields() {
    let adversarial: [&[&str]; 6] = [
        &[
            "plain",
            "a,b",
            "say \"hi\"",
            "\"",
            "\"\"",
            "line\nbreak",
            "cr\rhere",
            "crlf\r\nhere",
            " lead",
            "trail ",
            "\ttab",
            "",
            "caf\u{e9}",
        ],
        &[""],
        &["", ""],
        &["\"start", "end\"", ",", "\n", "\r", "\r\n"],
        &[",\",\n\"", " \" "],
        &["\u{ff}\u{20ac}", "'single'"],
    ];

    let unicode_only: [&[&str]; 1] = [&["\u{65e5}\u{672c}", "\u{1f600} x", "a\u{1f600},b"]];

    for encoding in [UTF_8, UTF_16LE, UTF_16BE, WINDOWS_1252] {
        let mut expected: Vec<Vec<String>> = Vec::new();

        // windows-1252 has no U+FEFF.
        if encoding != WINDOWS_1252 {
            expected.push(fields(&["\u{feff}"]));
            expected.push(fields(&["\u{feff}first", "\u{feff}"]));
        }

        expected.extend(adversarial.iter().map(|values| fields(values)));

        if encoding != WINDOWS_1252 {
            expected.extend(unicode_only.iter().map(|values| fields(values)));
        }

        let edits = EditSet {
            insertions: expected
                .iter()
                .map(|fields| Insertion {
                    position: InsertPosition::End,
                    fields: fields.clone(),
                })
                .collect(),
            ..EditSet::new(0)
        };

        let output = save(b"", dialect(encoding), &edits);

        assert_eq!(
            rows(&output, dialect(encoding)),
            expected,
            "{}",
            encoding.name()
        );
    }
}

#[test]
fn a_character_the_encoding_cannot_represent_is_refused() {
    let bytes = b"a,b\nc,d\n";

    let edits = EditSet {
        replacements: vec![replace(4..8, &["ok", "x\u{65e5}y"])],
        ..EditSet::new(0)
    };

    let (result, output) = try_save(bytes, dialect(WINDOWS_1252), &edits, 5);
    let error = result.unwrap_err();

    assert!(
        matches!(
            &error,
            WriteError::UnencodableCharacter {
                record: EditLocation::Existing(range),
                character: '\u{65e5}',
                encoding: "windows-1252",
            } if *range == (4..8)
        ),
        "{error:?}"
    );

    let message = error.to_string();

    assert!(message.contains("the record at bytes 4..8"), "{message}");
    assert!(message.contains("U+65E5"), "{message}");
    assert!(message.contains("windows-1252"), "{message}");
    assert!(output.is_empty());
}

#[test]
fn an_unencodable_appended_value_names_the_column_or_the_record() {
    let bytes = b"a\nb\n";

    let default_value = EditSet {
        appended_columns: vec![column("", "\u{3a9}", &[])],
        ..EditSet::new(0)
    };

    let record_value = EditSet {
        appended_columns: vec![column("", "", &[(2..4, "\u{3a9}")])],
        ..EditSet::new(0)
    };

    assert!(matches!(
        save_error(bytes, dialect(WINDOWS_1252), &default_value),
        WriteError::UnencodableCharacter {
            record: EditLocation::AppendedColumn(0),
            character: '\u{3a9}',
            ..
        }
    ));

    assert!(matches!(
        save_error(bytes, dialect(WINDOWS_1252), &record_value),
        WriteError::UnencodableCharacter {
            record: EditLocation::Existing(range),
            character: '\u{3a9}',
            ..
        } if range == (2..4)
    ));
}

#[test]
fn utf_16_output_is_real_utf_16_with_surrogate_pairs() {
    let edits = insert_at_end(&["a\u{1f600}"]);

    assert_eq!(
        save(b"", dialect(UTF_16LE), &edits),
        [0x61, 0x00, 0x3D, 0xD8, 0x00, 0xDE, 0x0A, 0x00]
    );

    assert_eq!(
        save(b"", dialect(UTF_16BE), &edits),
        [0x00, 0x61, 0xD8, 0x3D, 0xDE, 0x00, 0x00, 0x0A]
    );
}

#[test]
fn a_replaced_utf_16_record_keeps_its_two_unit_terminator() {
    let bytes = concat(&[b"\xFE\xFF", &utf16be("a,b\r\nc,d\r\ne,f\n")]);
    let records = ranges(&bytes, dialect(UTF_16BE));

    let edits = EditSet {
        replacements: vec![replace(records[1].clone(), &["\u{e9},x", "d"])],
        ..EditSet::new(0)
    };

    let expected = concat(&[b"\xFE\xFF", &utf16be("a,b\r\n\"\u{e9},x\",d\r\ne,f\n")]);

    assert_saves_as(&bytes, dialect(UTF_16BE), &edits, &expected);
}

#[test]
fn windows_1252_output_is_one_byte_per_character() {
    let edits = EditSet {
        replacements: vec![replace(0..5, &["caf\u{e9}", "\u{20ac}"])],
        ..EditSet::new(0)
    };

    assert_eq!(
        save(b"x,y\r\nz\r\n", dialect(WINDOWS_1252), &edits),
        b"caf\xE9,\x80\r\nz\r\n"
    );
}

#[test]
fn an_inconsistent_edit_set_is_refused_before_anything_is_written() {
    let bytes = b"a,b\nc,d\ne,f\n";

    let refused = |edits: EditSet| {
        let (result, output) = try_save(bytes, dialect(UTF_8), &edits, 5);
        assert!(output.is_empty());

        result.unwrap_err()
    };

    let replaced_twice = refused(EditSet {
        replacements: vec![replace(4..8, &["1"]), replace(4..8, &["2"])],
        ..EditSet::new(0)
    });
    assert!(
        matches!(&replaced_twice, WriteError::DuplicateEdit { range } if *range == (4..8)),
        "{replaced_twice:?}"
    );

    let deleted_twice = refused(EditSet {
        deletions: vec![4..8, 4..8],
        ..EditSet::new(0)
    });
    assert!(
        matches!(&deleted_twice, WriteError::DuplicateEdit { range } if *range == (4..8)),
        "{deleted_twice:?}"
    );

    let replaced_and_deleted = refused(EditSet {
        replacements: vec![replace(4..8, &["1"])],
        deletions: vec![4..8],
        ..EditSet::new(0)
    });
    assert!(
        matches!(
            &replaced_and_deleted,
            WriteError::ReplacedAndDeleted { range } if *range == (4..8)
        ),
        "{replaced_and_deleted:?}"
    );

    let overlapping = refused(EditSet {
        replacements: vec![replace(0..4, &["1"])],
        deletions: vec![2..8],
        ..EditSet::new(0)
    });
    assert!(
        matches!(
            &overlapping,
            WriteError::OverlappingEdits { first, second } if *first == (0..4) && *second == (2..8)
        ),
        "{overlapping:?}"
    );

    let same_start = refused(EditSet {
        replacements: vec![replace(4..8, &["1"])],
        deletions: vec![4..12],
        ..EditSet::new(0)
    });
    assert!(
        matches!(&same_start, WriteError::OverlappingEdits { .. }),
        "{same_start:?}"
    );

    let past_the_end = refused(EditSet {
        deletions: vec![8..13],
        ..EditSet::new(0)
    });
    assert!(
        matches!(
            &past_the_end,
            WriteError::RangeOutsideSource { range, source_length: 12 } if *range == (8..13)
        ),
        "{past_the_end:?}"
    );

    let empty_range = refused(EditSet {
        replacements: vec![replace(4..4, &["1"])],
        ..EditSet::new(0)
    });
    assert!(
        matches!(&empty_range, WriteError::RangeOutsideSource { .. }),
        "{empty_range:?}"
    );

    let insert_before_nothing = refused(EditSet {
        insertions: vec![insert(InsertPosition::Before(20..24), &["1"])],
        ..EditSet::new(0)
    });
    assert!(
        matches!(
            &insert_before_nothing,
            WriteError::RangeOutsideSource { .. }
        ),
        "{insert_before_nothing:?}"
    );

    let insert_inside_a_replacement = refused(EditSet {
        replacements: vec![replace(0..8, &["1"])],
        insertions: vec![insert(InsertPosition::Before(4..8), &["1"])],
        ..EditSet::new(0)
    });
    assert!(
        matches!(
            &insert_inside_a_replacement,
            WriteError::OverlappingEdits { .. }
        ),
        "{insert_inside_a_replacement:?}"
    );

    let value_given_twice = refused(EditSet {
        appended_columns: vec![column("", "", &[(4..8, "1"), (4..8, "2")])],
        ..EditSet::new(0)
    });
    assert!(
        matches!(&value_given_twice, WriteError::DuplicateEdit { range } if *range == (4..8)),
        "{value_given_twice:?}"
    );
}

#[test]
fn an_edit_inside_the_byte_order_mark_is_refused() {
    let edits = EditSet {
        deletions: vec![0..5],
        ..EditSet::new(0)
    };

    let error = save_error(b"\xEF\xBB\xBFa\n", dialect(UTF_8), &edits);

    assert!(
        matches!(&error, WriteError::NotARecord { range } if *range == (0..5)),
        "{error:?}"
    );
}

#[test]
fn appending_a_column_refuses_an_edit_that_is_not_a_whole_record() {
    let edits = EditSet {
        appended_columns: vec![column("", "", &[])],
        deletions: vec![5..8],
        ..EditSet::new(0)
    };

    let error = save_error(b"a,b\nc,d\ne,f\n", dialect(UTF_8), &edits);

    assert!(
        matches!(&error, WriteError::NotARecord { range } if *range == (5..8)),
        "{error:?}"
    );

    let wrong_end = EditSet {
        appended_columns: vec![column("", "", &[])],
        deletions: vec![4..7],
        ..EditSet::new(0)
    };

    let error = save_error(b"a,b\nc,d\ne,f\n", dialect(UTF_8), &wrong_end);

    assert!(
        matches!(&error, WriteError::NotARecord { range } if *range == (4..7)),
        "{error:?}"
    );
}

#[test]
fn a_source_error_propagates_with_its_message() {
    for (good_reads, edits) in [
        (0, EditSet::new(MIXED.len() as u64)),
        (1, EditSet::new(MIXED.len() as u64)),
        (
            1,
            EditSet {
                appended_columns: vec![column("", "", &[])],
                ..EditSet::new(MIXED.len() as u64)
            },
        ),
    ] {
        let source = FlakySource {
            bytes: MIXED.to_vec(),
            good_reads: RefCell::new(good_reads),
            failure: Failure::Error("the bucket is gone"),
        };

        let error = write_edited(
            &source,
            &dialect(UTF_8),
            &edits,
            NonZeroU64::new(4).unwrap(),
            &mut Vec::new(),
        )
        .unwrap_err();

        assert!(matches!(error, WriteError::Source(_)), "{error:?}");
        assert_eq!(error.to_string(), "the bucket is gone");
    }
}

#[test]
fn a_source_that_returns_too_few_bytes_while_copying_is_refused() {
    let source = FlakySource {
        bytes: MIXED.to_vec(),
        good_reads: RefCell::new(2),
        failure: Failure::OneByteShort,
    };

    let error = write_edited(
        &source,
        &dialect(UTF_8),
        &EditSet::new(MIXED.len() as u64),
        NonZeroU64::new(4).unwrap(),
        &mut Vec::new(),
    )
    .unwrap_err();

    assert!(
        matches!(
            error,
            WriteError::Read(ReadError::UnexpectedReadLength {
                start: 4,
                end: 8,
                received: 3
            })
        ),
        "{error:?}"
    );
}

#[test]
fn a_sink_error_propagates_with_its_message() {
    let source = MemorySource::new(MIXED.to_vec());

    let error = write_edited(
        &source,
        &dialect(UTF_8),
        &EditSet::new(MIXED.len() as u64),
        NonZeroU64::new(4).unwrap(),
        &mut FullSink { capacity: 6 },
    )
    .unwrap_err();

    assert!(matches!(error, WriteError::Sink(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "could not write the output: the disk is full"
    );
}

#[test]
fn a_dialect_the_reader_refuses_is_refused_by_the_writer() {
    let dialect = Dialect {
        delimiter: b'"',
        ..dialect(UTF_8)
    };

    let error = save_error(b"a\n", dialect, &EditSet::new(MIXED.len() as u64));

    assert!(
        matches!(
            error,
            WriteError::Read(ReadError::QuoteEqualsDelimiter { byte: b'"' })
        ),
        "{error:?}"
    );
}

const ROSTER: &[u8] = b"id,name\n1,alice\n2,bob\n3,carol\n";

/// Checks that saving `bytes` with `edits` is refused before a byte is
/// written, and returns the error.
fn refused(bytes: &[u8], dialect: Dialect, edits: &EditSet) -> WriteError {
    for window_size in 1..=bytes.len() as u64 + 1 {
        let (result, output) = try_save(bytes, dialect, edits, window_size);

        assert!(result.is_err(), "window size {window_size}");
        assert!(output.is_empty(), "window size {window_size}");
    }

    save_error(bytes, dialect, edits)
}

#[test]
fn a_field_that_starts_with_the_mark_character_is_quoted() {
    let replaced = EditSet {
        replacements: vec![replace(0..4, &["\u{feff}a", "b"])],
        ..EditSet::new(0)
    };

    let output = save(b"a,b\n1,2\n", dialect(UTF_8), &replaced);

    assert_eq!(output, b"\"\xEF\xBB\xBFa\",b\n1,2\n");
    assert_eq!(
        rows(&output, dialect(UTF_8)),
        [fields(&["\u{feff}a", "b"]), fields(&["1", "2"])]
    );

    let inserted_first = EditSet {
        insertions: vec![insert(InsertPosition::Before(0..4), &["\u{feff}a", "b"])],
        ..EditSet::new(0)
    };

    let output = save(b"a,b\n1,2\n", dialect(UTF_8), &inserted_first);

    assert_eq!(
        rows(&output, dialect(UTF_8))[0],
        fields(&["\u{feff}a", "b"])
    );

    for encoding in [UTF_8, UTF_16LE, UTF_16BE] {
        let output = save(b"", dialect(encoding), &insert_at_end(&["\u{feff}"]));

        assert_eq!(
            rows(&output, dialect(encoding)),
            [fields(&["\u{feff}"])],
            "{}",
            encoding.name()
        );
    }
}

#[test]
fn without_a_quote_a_field_that_starts_with_the_mark_character_is_refused() {
    let error = refused(
        b"x,y\n",
        without_quote(dialect(UTF_8)),
        &insert_at_end(&["ok", "\u{feff}a"]),
    );

    assert!(
        matches!(
            &error,
            WriteError::UnquotableField {
                record: EditLocation::Inserted(0),
                field: 1
            }
        ),
        "{error:?}"
    );
}

#[test]
fn a_copied_record_that_would_start_the_file_with_a_mark_is_refused() {
    let bytes = b"a\n\xEF\xBB\xBFb\nc\n";

    let delete_first = EditSet {
        deletions: vec![0..2],
        ..EditSet::new(0)
    };

    let error = refused(bytes, dialect(UTF_8), &delete_first);

    assert!(
        matches!(error, WriteError::LeadingByteOrderMark { offset: 2 }),
        "{error:?}"
    );
    assert!(error.to_string().contains("byte 2"), "{error}");

    let delete_first_and_replace_next = EditSet {
        deletions: vec![0..2],
        replacements: vec![replace(2..7, &["\u{feff}b"])],
        ..EditSet::new(0)
    };

    let output = save(bytes, dialect(UTF_8), &delete_first_and_replace_next);

    assert_eq!(
        rows(&output, dialect(UTF_8)),
        [fields(&["\u{feff}b"]), fields(&["c"])]
    );
}

#[test]
fn a_copied_record_that_starts_with_the_mark_character_is_kept_after_a_real_mark() {
    let bytes = b"\xEF\xBB\xBFa\n\xEF\xBB\xBFb\n";

    let edits = EditSet {
        deletions: vec![3..5],
        ..EditSet::new(0)
    };

    let output = save(bytes, dialect(UTF_8), &edits);

    assert_eq!(output, b"\xEF\xBB\xBF\xEF\xBB\xBFb\n");
    assert_eq!(rows(&output, dialect(UTF_8)), [fields(&["\u{feff}b"])]);
}

#[test]
fn deleting_between_a_carriage_return_and_an_empty_line_keeps_the_empty_record() {
    let delete_b = EditSet {
        deletions: vec![2..4],
        ..EditSet::new(0)
    };

    let expected = b"a\r\"\"\nc\n";

    assert_saves_as(b"a\rb\n\nc\n", dialect(UTF_8), &delete_b, expected);
    assert_eq!(
        rows(expected, dialect(UTF_8)),
        [fields(&["a"]), fields(&[""]), fields(&["c"])]
    );

    let two_empty_lines = b"a\r\"\"\n\nc";

    assert_saves_as(b"a\rb\n\n\nc", dialect(UTF_8), &delete_b, two_empty_lines);
    assert_eq!(
        rows(two_empty_lines, dialect(UTF_8)),
        [fields(&["a"]), fields(&[""]), fields(&[""]), fields(&["c"])]
    );

    let utf16 = utf16le("a\rb\n\nc\n");

    let delete_b_utf16 = EditSet {
        deletions: vec![4..8],
        ..EditSet::new(0)
    };

    assert_saves_as(
        &utf16,
        dialect(UTF_16LE),
        &delete_b_utf16,
        &utf16le("a\r\"\"\nc\n"),
    );
}

#[test]
fn a_replaced_record_ending_in_a_carriage_return_does_not_fuse_with_an_empty_line() {
    let edits = EditSet {
        replacements: vec![replace(0..2, &["z"])],
        deletions: vec![2..4],
        ..EditSet::new(0)
    };

    assert_saves_as(b"a\rb\n\nc\n", dialect(UTF_8), &edits, b"z\r\"\"\nc\n");
}

#[test]
fn a_record_inserted_in_the_gap_keeps_the_line_breaks_apart_without_a_change() {
    let edits = EditSet {
        insertions: vec![insert(InsertPosition::Before(2..4), &["i"])],
        deletions: vec![2..4],
        ..EditSet::new(0)
    };

    assert_saves_as(b"a\rb\n\nc\n", dialect(UTF_8), &edits, b"a\ri\n\nc\n");
}

#[test]
fn without_a_quote_a_deletion_that_would_fuse_line_breaks_is_refused() {
    let edits = EditSet {
        deletions: vec![2..4],
        ..EditSet::new(0)
    };

    let error = save_error(b"a\rb\n\nc\n", without_quote(dialect(UTF_8)), &edits);

    assert!(
        matches!(error, WriteError::FusedLineBreak { offset: 4 }),
        "{error:?}"
    );
}

#[test]
fn appending_a_column_while_deleting_keeps_an_empty_line_a_record() {
    let edits = EditSet {
        deletions: vec![2..4],
        appended_columns: vec![column("", "v", &[])],
        ..EditSet::new(0)
    };

    let expected = b"a,v\r,v\nc,v\n";

    assert_saves_as(b"a\rb\n\nc\n", dialect(UTF_8), &edits, expected);
    assert_eq!(
        rows(expected, dialect(UTF_8)),
        [fields(&["a", "v"]), fields(&["", "v"]), fields(&["c", "v"])]
    );
}

#[test]
fn a_range_that_is_not_one_record_is_refused_before_anything_is_written() {
    let not_a_record = |edits: EditSet, range: Range<u64>| {
        let error = refused(ROSTER, dialect(UTF_8), &edits);

        assert!(
            matches!(&error, WriteError::NotARecord { range: refused } if *refused == range),
            "{range:?}: {error:?}"
        );
    };

    for range in [10..18, 8..22, 8..12, 9..16] {
        not_a_record(
            EditSet {
                replacements: vec![replace(range.clone(), &["X", "Y"])],
                ..EditSet::new(0)
            },
            range.clone(),
        );

        not_a_record(
            EditSet {
                deletions: vec![range.clone()],
                ..EditSet::new(0)
            },
            range.clone(),
        );

        not_a_record(
            EditSet {
                insertions: vec![insert(InsertPosition::Before(range.clone()), &["X"])],
                ..EditSet::new(0)
            },
            range.clone(),
        );

        not_a_record(
            EditSet {
                appended_columns: vec![column("", "", &[(range.clone(), "X")])],
                ..EditSet::new(0)
            },
            range,
        );
    }
}

#[test]
fn a_range_that_splits_a_carriage_return_from_its_line_feed_is_refused() {
    let bytes = b"a\r\nb\r\nc\r\n";

    for range in [0..2, 2..3, 2..5, 3..4, 4..6] {
        let edits = EditSet {
            deletions: vec![range.clone()],
            ..EditSet::new(0)
        };

        let error = refused(bytes, dialect(UTF_8), &edits);

        assert!(
            matches!(&error, WriteError::NotARecord { range: refused } if *refused == range),
            "{range:?}: {error:?}"
        );
    }
}

#[test]
fn a_utf_16_range_that_starts_inside_a_code_unit_is_refused() {
    // Read one byte off, the two bytes before offset 3 are a line feed and
    // the rest of the source has no line break.
    let bytes = utf16le("\u{0a41}\u{4200}cd");

    assert_eq!(bytes[1..3], [0x0A, 0x00]);

    let edits = EditSet {
        deletions: vec![3..8],
        ..EditSet::new(0)
    };

    let error = refused(&bytes, dialect(UTF_16LE), &edits);

    assert!(
        matches!(&error, WriteError::NotARecord { range } if *range == (3..8)),
        "{error:?}"
    );
}

#[test]
fn a_source_of_another_length_than_the_edit_set_names_is_refused() {
    let source = MemorySource::new(ROSTER.to_vec());

    for edits in [
        EditSet::new(ROSTER.len() as u64 + 1),
        EditSet {
            deletions: vec![8..16],
            ..EditSet::new(0)
        },
    ] {
        let mut output = Vec::new();

        let error = write_edited(
            &source,
            &dialect(UTF_8),
            &edits,
            NonZeroU64::new(4).unwrap(),
            &mut output,
        )
        .unwrap_err();

        assert!(
            matches!(
                error,
                WriteError::SourceChanged { expected, actual: 30 }
                    if expected == edits.source_length
            ),
            "{error:?}"
        );
        assert!(error.to_string().contains("reload the file"), "{error}");
        assert!(output.is_empty());
    }
}

#[test]
fn a_range_inside_a_quoted_field_is_caught_only_when_a_column_is_appended() {
    let bytes = b"\"x\ny\",1\nb,2\n";

    let edits = EditSet {
        deletions: vec![3..8],
        appended_columns: vec![column("", "", &[])],
        ..EditSet::new(0)
    };

    let error = save_error(bytes, dialect(UTF_8), &edits);

    assert!(
        matches!(&error, WriteError::NotARecord { range } if *range == (3..8)),
        "{error:?}"
    );
}

#[test]
fn a_utf_16_source_that_ends_inside_a_code_unit_is_only_copied() {
    let bytes = concat(&[b"\xFF\xFE", &utf16le("a\nb"), b"\x41"]);
    let dialect = dialect(UTF_16LE);

    assert_saves_as(&bytes, dialect, &EditSet::new(0), &bytes);

    let delete_first = EditSet {
        deletions: vec![2..6],
        ..EditSet::new(0)
    };

    let appended = EditSet {
        appended_columns: vec![column("", "x", &[])],
        ..EditSet::new(0)
    };

    for edits in [delete_first, insert_at_end(&["n"]), appended] {
        let error = refused(&bytes, dialect, &edits);

        assert!(
            matches!(error, WriteError::TruncatedCodeUnit { source_length: 9 }),
            "{error:?}"
        );
    }

    let delete_last = EditSet {
        deletions: vec![6..9],
        ..EditSet::new(0)
    };

    assert_saves_as(
        &bytes,
        dialect,
        &delete_last,
        &concat(&[b"\xFF\xFE", &utf16le("a\n")]),
    );

    let replace_last_and_insert = EditSet {
        replacements: vec![replace(6..9, &["z"])],
        insertions: vec![insert(InsertPosition::End, &["n"])],
        ..EditSet::new(0)
    };

    assert_saves_as(
        &bytes,
        dialect,
        &replace_last_and_insert,
        &concat(&[b"\xFF\xFE", &utf16le("a\nz\nn\n")]),
    );
}

#[test]
fn an_insertion_before_a_trailing_half_code_unit_is_refused() {
    let insert_before_the_half_unit = concat(&[&utf16le("a\rb\n\nc\n"), b"\x41"]);

    let edits = EditSet {
        insertions: vec![insert(InsertPosition::Before(14..15), &["x", "y"])],
        ..EditSet::new(0)
    };

    let error = refused(&insert_before_the_half_unit, dialect(UTF_16LE), &edits);

    assert!(
        matches!(error, WriteError::TruncatedCodeUnit { source_length: 15 }),
        "{error:?}"
    );
}

#[test]
fn a_deletion_before_a_trailing_half_code_unit_is_refused() {
    let delete_before_the_half_unit = concat(&[&utf16le("\r\n\n\r\r\n"), b"\x41"]);

    let edits = EditSet {
        deletions: vec![8..12],
        ..EditSet::new(0)
    };

    let error = refused(&delete_before_the_half_unit, dialect(UTF_16LE), &edits);

    assert!(
        matches!(error, WriteError::TruncatedCodeUnit { source_length: 13 }),
        "{error:?}"
    );
}

#[test]
fn a_column_name_is_not_rendered_without_a_header() {
    let edits = EditSet {
        appended_columns: vec![column("\u{65e5}", "v", &[])],
        ..EditSet::new(0)
    };

    assert_eq!(
        save(b"a\nb\n", dialect(WINDOWS_1252), &edits),
        b"a,v\nb,v\n"
    );

    let error = save_error(b"a\nb\n", with_header(dialect(WINDOWS_1252)), &edits);

    assert!(
        matches!(
            error,
            WriteError::UnencodableCharacter {
                record: EditLocation::AppendedColumn(0),
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn an_appended_value_for_a_deleted_or_replaced_record_is_ignored() {
    let edits = EditSet {
        deletions: vec![0..2],
        replacements: vec![replace(2..4, &["r"])],
        appended_columns: vec![column("", "v", &[(0..2, "\u{65e5}"), (2..4, "\u{65e5}")])],
        ..EditSet::new(0)
    };

    assert_eq!(
        save(b"a\nb\nc\n", dialect(WINDOWS_1252), &edits),
        b"r\nc,v\n"
    );
}

#[test]
fn appended_value_ranges_are_checked_like_every_other_range() {
    let same_start = EditSet {
        appended_columns: vec![
            column("", "", &[(0..2, "x")]),
            column("", "", &[(0..3, "y")]),
        ],
        ..EditSet::new(0)
    };

    let error = refused(b"a\nb\nc\n", dialect(UTF_8), &same_start);

    assert!(
        matches!(
            &error,
            WriteError::OverlappingEdits { first, second } if *first == (0..2) && *second == (0..3)
        ),
        "{error:?}"
    );

    let overlapping_a_deletion = EditSet {
        deletions: vec![2..4],
        appended_columns: vec![column("", "", &[(0..3, "y")])],
        ..EditSet::new(0)
    };

    let error = refused(b"a\nb\nc\n", dialect(UTF_8), &overlapping_a_deletion);

    assert!(
        matches!(&error, WriteError::OverlappingEdits { .. }),
        "{error:?}"
    );

    let past_the_end = EditSet {
        appended_columns: vec![column("", "", &[(4..9, "y")])],
        ..EditSet::new(0)
    };

    let error = refused(b"a\nb\nc\n", dialect(UTF_8), &past_the_end);

    assert!(
        matches!(&error, WriteError::RangeOutsideSource { .. }),
        "{error:?}"
    );
}

// -- Rendering one record ------------------------------------------------------

/// Field sets that take every quoting rule: plain, the delimiter, a quote, a
/// line break, padding, a leading U+FEFF, an empty field and the empty only
/// field.
fn field_sets() -> Vec<Vec<String>> {
    vec![
        fields(&["plain", "text"]),
        fields(&["a,b", "say \"hi\""]),
        fields(&["two\nlines", "cr\ronly"]),
        fields(&[" padded", "tail "]),
        fields(&["\u{feff}mark", ""]),
        fields(&[""]),
        Vec::new(),
    ]
}

#[test]
fn a_rendered_record_is_the_replacement_the_writer_writes() {
    let source = b"x\r\ny\n";

    for encoding in [UTF_8, UTF_16LE, UTF_16BE, WINDOWS_1252] {
        let dialect = dialect(encoding);
        let bytes = match encoding {
            encoding if encoding == UTF_16LE => utf16le("x\r\ny\n"),
            encoding if encoding == UTF_16BE => utf16be("x\r\ny\n"),
            _ => source.to_vec(),
        };
        let ranges = ranges(&bytes, dialect);

        for values in field_sets() {
            // windows-1252 has no U+FEFF.
            if encoding == WINDOWS_1252 && values.iter().any(|value| value.contains('\u{feff}')) {
                continue;
            }

            let edits = EditSet {
                replacements: vec![Replacement {
                    byte_range: ranges[0].clone(),
                    fields: values.clone(),
                }],
                ..EditSet::new(0)
            };

            let saved = save(&bytes, dialect, &edits);

            let location = EditLocation::Existing(ranges[0].clone());
            let rendered = super::render_record(&values, &dialect, &location).unwrap();

            let terminator = match encoding {
                encoding if encoding == UTF_16LE => utf16le("\r\n"),
                encoding if encoding == UTF_16BE => utf16be("\r\n"),
                _ => b"\r\n".to_vec(),
            };
            let tail = &bytes[ranges[1].start as usize..];

            assert_eq!(
                saved,
                concat(&[&rendered, &terminator, tail]),
                "{} {values:?}",
                encoding.name()
            );
        }
    }
}

#[test]
fn rendered_appended_fields_are_the_fields_the_writer_appends() {
    let bytes = b"a,b\n1,2\n";
    let dialect = with_header(dialect(UTF_8));
    let ranges = ranges(bytes, dialect);

    for values in [
        fields(&["plain", ""]),
        fields(&["a,b", " padded"]),
        fields(&["two\nlines", "say \"hi\""]),
    ] {
        let edits = EditSet {
            appended_columns: values
                .iter()
                .map(|value| column("name", "", &[(ranges[0].clone(), value.as_str())]))
                .collect(),
            ..EditSet::new(0)
        };

        let saved = save(bytes, dialect, &edits);

        let location = EditLocation::Existing(ranges[0].clone());
        let appended = super::render_appended_fields(&values, &dialect, &location).unwrap();

        let headers = super::render_appended_fields(
            &vec!["name".to_string(); values.len()],
            &dialect,
            &EditLocation::AppendedColumn(0),
        )
        .unwrap();

        assert_eq!(
            saved,
            concat(&[b"a,b", &headers, b"\n1,2", &appended, b"\n"]),
            "{values:?}"
        );
    }
}

#[test]
fn rendering_refuses_what_the_writer_refuses() {
    let location = EditLocation::Inserted(0);

    let error =
        super::render_record(&fields(&["\u{20ac}"]), &dialect(WINDOWS_1252), &location).err();
    assert!(error.is_none(), "{error:?}");

    let error =
        super::render_record(&fields(&["\u{101}"]), &dialect(WINDOWS_1252), &location).unwrap_err();
    assert!(
        matches!(
            error,
            WriteError::UnencodableCharacter {
                character: '\u{101}',
                ..
            }
        ),
        "{error:?}"
    );

    let error =
        super::render_appended_fields(&fields(&["a,b"]), &without_quote(dialect(UTF_8)), &location)
            .unwrap_err();
    assert!(
        matches!(error, WriteError::UnquotableField { .. }),
        "{error:?}"
    );

    let line_break = Dialect {
        delimiter: b'\n',
        ..dialect(UTF_8)
    };
    let error = super::render_record(&fields(&["a"]), &line_break, &location).unwrap_err();
    assert!(
        matches!(
            error,
            WriteError::Read(ReadError::LineBreakInDialect { .. })
        ),
        "{error:?}"
    );
}
