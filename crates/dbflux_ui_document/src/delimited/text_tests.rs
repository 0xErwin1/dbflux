use std::num::{NonZeroU64, NonZeroUsize};

use dbflux_components::components::data_table::model::{CellValue, EditBuffer, InsertAnchor};
use dbflux_delimited::{
    ByteSource, Dialect, Encoding, MemorySource, PagedReader, ReaderOptions, decode, write_edited,
};

use super::page_model::PageModel;
use super::text::{
    ALIGNED_MAX_WIDTH, RenderedText, SourceSpan, TextEnd, TextLimits, TextRow, render_aligned,
    render_raw,
};

/// Limits no fixture reaches.
const NO_LIMITS: TextLimits = TextLimits {
    max_bytes: usize::MAX,
    max_lines: usize::MAX,
};

fn utf_8() -> &'static Encoding {
    Encoding::for_label(b"utf-8").expect("a known encoding label")
}

fn dialect(has_header: bool, encoding: &'static Encoding) -> Dialect {
    Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header,
        encoding,
    }
}

/// The loaded part of a file: its page model, the bytes the document keeps
/// for it, and what a save needs.
struct Loaded {
    bytes: Vec<u8>,
    dialect: Dialect,
    model: PageModel,
    span: SourceSpan,
}

impl Loaded {
    /// An edit buffer over the loaded rows, as the table holds it.
    fn buffer(&self) -> EditBuffer {
        let mut buffer = EditBuffer::new();
        buffer.set_base_row_count(self.model.records().len());
        buffer
    }

    fn raw(&self, edits: &EditBuffer) -> RenderedText {
        let rendered = render_raw(&self.model, &self.span, edits, &self.dialect, NO_LIMITS)
            .expect("the raw text renders");

        assert_tiles(&rendered);
        rendered
    }

    /// The file a save with `edits` writes, decoded the way the text view
    /// decodes it.
    fn saved_text(&self, edits: &EditBuffer) -> String {
        let source = MemorySource::new(self.bytes.clone());
        let edit_set = self
            .model
            .edit_set(self.bytes.len() as u64, edits)
            .expect("the edit set builds");

        let mut output = Vec::new();

        write_edited(
            &source,
            &self.dialect,
            &edit_set,
            NonZeroU64::new(7).expect("a non-zero window"),
            &mut output,
        )
        .expect("the edited file writes");

        decode(&output, &self.dialect).text.into_owned()
    }
}

/// Every byte of the text belongs to exactly one record, in order.
fn assert_tiles(rendered: &RenderedText) {
    let mut end = 0;

    for record in &rendered.layout.records {
        assert_eq!(
            record.text.start, end,
            "{:?} in {:?}",
            record.row, rendered.text
        );
        end = record.text.end;
    }

    assert_eq!(
        end,
        rendered.text.len(),
        "the text ends with a record: {:?}",
        rendered.text
    );
}

/// Reads the first `pages` pages of `bytes`, `page_size` records each, and
/// keeps the bytes of everything read.
fn load(bytes: &[u8], dialect: Dialect, page_size: usize, pages: usize) -> Loaded {
    let options = ReaderOptions {
        page_size: NonZeroUsize::new(page_size).expect("a non-zero page size"),
        window_size: NonZeroU64::new(5).expect("a non-zero window"),
    };

    let mut reader = PagedReader::open(MemorySource::new(bytes.to_vec()), dialect, options)
        .expect("the reader opens");
    let mut model = PageModel::new(reader.header().cloned());

    for page_index in 0..pages {
        let page = reader.read_page(page_index).expect("the page reads");

        model
            .append_page(page, reader.record_count())
            .expect("the page follows the loaded records");
    }

    let start = reader.byte_order_mark_length();
    let end = model
        .records()
        .last()
        .or(model.header())
        .map_or(start, |record| record.byte_range.end);

    let kept = reader
        .source()
        .read_range(start..end)
        .expect("the loaded bytes read");

    Loaded {
        bytes: bytes.to_vec(),
        dialect,
        model,
        span: SourceSpan::new(start, kept),
    }
}

fn load_whole(bytes: &[u8], dialect: Dialect) -> Loaded {
    load(bytes, dialect, 100, 1)
}

fn stage(buffer: &mut EditBuffer, row: usize, column: usize, value: &str) {
    buffer.set_cell(row, column, CellValue::text(value));
}

fn text_row(values: &[&str]) -> Vec<CellValue> {
    values.iter().map(|value| CellValue::text(value)).collect()
}

// -- Raw: an untouched file ----------------------------------------------------

#[test]
fn the_raw_text_of_an_untouched_file_is_the_file_decoded() {
    let windows_1252 = Encoding::for_label(b"windows-1252").expect("a known encoding label");
    let utf_16le = Encoding::for_label(b"utf-16le").expect("a known encoding label");

    let utf_16_file: Vec<u8> = b"\xFF\xFE"
        .iter()
        .copied()
        .chain(
            "a,b\r\n1,\"x\ny\"\n"
                .encode_utf16()
                .flat_map(u16::to_le_bytes),
        )
        .collect();

    let cases: Vec<(&str, Vec<u8>, Dialect)> = vec![
        ("LF", b"a,b\n1,2\n3,4\n".to_vec(), dialect(true, utf_8())),
        (
            "CRLF",
            b"a,b\r\n1,2\r\n3,4\r\n".to_vec(),
            dialect(true, utf_8()),
        ),
        (
            "mixed endings",
            b"a,b\r\n1,2\n3,4\r5,6\r\n7,8".to_vec(),
            dialect(true, utf_8()),
        ),
        (
            "quoted line breaks",
            b"a,b\n\"one\r\ntwo\",\"x\ny\"\n3,\"\"\"q\"\"\"\n".to_vec(),
            dialect(true, utf_8()),
        ),
        (
            "byte-order mark",
            b"\xEF\xBB\xBFa,b\n1,2\n".to_vec(),
            dialect(true, utf_8()),
        ),
        (
            "windows-1252",
            b"name;city\nJos\xE9;M\xE1laga\n".to_vec(),
            Dialect {
                delimiter: b';',
                ..dialect(true, windows_1252)
            },
        ),
        ("UTF-16 LE", utf_16_file, dialect(true, utf_16le)),
        (
            "headerless",
            b"1,2\n\n3,4\n".to_vec(),
            dialect(false, utf_8()),
        ),
        ("empty", Vec::new(), dialect(true, utf_8())),
    ];

    for (name, bytes, dialect) in cases {
        let loaded = load_whole(&bytes, dialect);
        let rendered = loaded.raw(&loaded.buffer());

        assert_eq!(
            rendered.text,
            decode(&bytes, &dialect).text,
            "{name}: the raw text is the file decoded"
        );
        assert!(!rendered.text.starts_with('\u{feff}'), "{name}");
        assert_eq!(rendered.layout.end, TextEnd::Complete, "{name}");
    }
}

#[test]
fn a_partly_loaded_file_shows_its_loaded_span_and_says_more_follows() {
    let bytes = b"id\r\n0\r\n1\r\n2\r\n3\r\n4\r\n";
    let loaded = load(bytes, dialect(true, utf_8()), 2, 2);

    let rendered = loaded.raw(&loaded.buffer());

    assert_eq!(rendered.text, "id\r\n0\r\n1\r\n2\r\n3\r\n");
    assert_eq!(rendered.layout.end, TextEnd::MoreInFile);
}

// -- Raw: pending changes --------------------------------------------------------

const PEOPLE: &[u8] = b"name,city\r\nAna,Lima\r\n\"Bo, Jr\",\"Qui\nto\"\nCy,Rome\r\nDi,Oslo";

#[test]
fn the_raw_text_of_pending_changes_is_what_a_save_writes() {
    type Edit = fn(&mut Loaded, &mut EditBuffer);

    let edits: Vec<(&str, Edit)> = vec![
        ("a cell edit", |_, buffer| stage(buffer, 1, 0, "Bob \"B\"")),
        ("an insert above the first row", |_, buffer| {
            buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_row(&["New", "Town"]));
        }),
        ("an insert after a row", |_, buffer| {
            buffer.add_pending_insert_at(InsertAnchor::After(1), text_row(&["Mid", "a,b"]));
        }),
        ("an insert at the end", |_, buffer| {
            buffer.add_pending_insert_at(InsertAnchor::After(3), text_row(&["Ed", "Kyiv"]));
        }),
        ("a delete", |_, buffer| buffer.mark_for_delete(1)),
        ("a delete of the last record", |_, buffer| {
            buffer.mark_for_delete(3)
        }),
        ("an added column", |loaded, buffer| {
            loaded
                .model
                .append_column("age".to_string())
                .expect("a fully loaded file takes a column");
            stage(buffer, 2, 2, "41");
        }),
        ("a rename", |loaded, _| {
            loaded
                .model
                .rename_column(1, "town".to_string())
                .expect("a file with a header renames");
        }),
        ("everything at once", |loaded, buffer| {
            loaded
                .model
                .append_column("age".to_string())
                .expect("a fully loaded file takes a column");
            loaded
                .model
                .rename_column(0, "who".to_string())
                .expect("a file with a header renames");
            stage(buffer, 0, 1, "Cusco");
            stage(buffer, 3, 2, " 7");
            buffer.mark_for_delete(2);
            buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_row(&["Zed", "", ""]));
            buffer.add_pending_insert_at(InsertAnchor::After(3), text_row(&["Ed", "Kyiv", "9"]));
        }),
    ];

    for (name, edit) in edits {
        let mut loaded = load_whole(PEOPLE, dialect(true, utf_8()));
        let mut buffer = loaded.buffer();

        edit(&mut loaded, &mut buffer);

        assert_eq!(
            loaded.raw(&buffer).text,
            loaded.saved_text(&buffer),
            "{name}"
        );
    }
}

#[test]
fn a_deletion_that_would_fuse_line_breaks_shows_the_quoted_empty_record() {
    let bytes = b"a\r\nb\n\nc\n";
    let loaded = load_whole(bytes, dialect(false, utf_8()));

    let mut buffer = loaded.buffer();
    buffer.mark_for_delete(1);

    let bare_cr = load_whole(b"a\rb\n\nc\n", dialect(false, utf_8()));

    let mut deletes_b = bare_cr.buffer();
    deletes_b.mark_for_delete(1);

    assert_eq!(loaded.raw(&buffer).text, loaded.saved_text(&buffer));
    assert_eq!(bare_cr.raw(&deletes_b).text, "a\r\"\"\nc\n");
    assert_eq!(bare_cr.raw(&deletes_b).text, bare_cr.saved_text(&deletes_b));
}

#[test]
fn a_value_the_encoding_cannot_hold_is_the_error_a_save_would_report() {
    let windows_1252 = Encoding::for_label(b"windows-1252").expect("a known encoding label");
    let loaded = load_whole(b"a,b\n1,2\n", dialect(true, windows_1252));

    let mut buffer = loaded.buffer();
    stage(&mut buffer, 0, 0, "\u{101}");

    let error = render_raw(
        &loaded.model,
        &loaded.span,
        &buffer,
        &loaded.dialect,
        NO_LIMITS,
    )
    .expect_err("the record cannot be encoded");

    assert!(error.to_string().contains("windows-1252"), "{error}");
}

// -- Raw: the map from records to rows -------------------------------------------

#[test]
fn every_rendered_record_names_its_row_and_its_lines() {
    let loaded = load_whole(PEOPLE, dialect(true, utf_8()));

    let mut buffer = loaded.buffer();
    buffer.add_pending_insert_at(InsertAnchor::After(0), text_row(&["New", "Town"]));
    buffer.mark_for_delete(2);

    let rendered = loaded.raw(&buffer);

    let rows: Vec<TextRow> = rendered
        .layout
        .records
        .iter()
        .map(|record| record.row)
        .collect();
    assert_eq!(
        rows,
        [
            TextRow::Header,
            TextRow::Base(0),
            TextRow::Insert(0),
            TextRow::Base(1),
            TextRow::Base(3),
        ]
    );

    let lines: Vec<_> = rendered
        .layout
        .records
        .iter()
        .map(|record| record.lines.clone())
        .collect();
    assert_eq!(lines, [0..1, 1..2, 2..3, 3..5, 5..6]);

    for record in &rendered.layout.records {
        let text = &rendered.text[record.text.clone()];
        assert!(!text.is_empty(), "{:?}", record.row);
    }

    assert_eq!(
        &rendered.text[rendered.layout.records[3].text.clone()],
        "\"Bo, Jr\",\"Qui\nto\"\n"
    );
    assert_eq!(rendered.layout.line_of(TextRow::Base(3)), Some(5));
    assert_eq!(rendered.layout.line_of(TextRow::Base(2)), None);

    let bo = rendered.layout.records[3].text.clone();
    assert_eq!(
        rendered.layout.row_at(bo.start + 3),
        Some((TextRow::Base(1), 3))
    );
    assert_eq!(
        rendered.layout.row_at(rendered.text.len()),
        Some((TextRow::Base(3), rendered.layout.records[4].text.len()))
    );
    assert_eq!(
        rendered.layout.offset_in(TextRow::Base(1), 3),
        Some(bo.start + 3)
    );
    assert_eq!(
        rendered.layout.offset_in(TextRow::Base(1), 500),
        Some(bo.end - 1)
    );
    assert_eq!(rendered.layout.offset_in(TextRow::Base(2), 0), None);
}

// -- Raw and aligned: the limits -------------------------------------------------

#[test]
fn the_text_stops_at_a_record_once_it_reaches_its_limits() {
    let mut bytes = b"id,city\n".to_vec();

    for record in 0..100 {
        bytes.extend_from_slice(format!("{record},Lima\n").as_bytes());
    }

    let loaded = load_whole(&bytes, dialect(true, utf_8()));
    let buffer = loaded.buffer();

    let by_lines = TextLimits {
        max_bytes: usize::MAX,
        max_lines: 11,
    };
    let rendered = render_raw(
        &loaded.model,
        &loaded.span,
        &buffer,
        &loaded.dialect,
        by_lines,
    )
    .expect("the raw text renders");

    assert_eq!(rendered.layout.end, TextEnd::Capped { shown: 10 });
    assert_eq!(rendered.text.lines().count(), 11);
    assert!(rendered.text.ends_with("9,Lima\n"), "{}", rendered.text);

    let by_bytes = TextLimits {
        max_bytes: 30,
        max_lines: usize::MAX,
    };
    let rendered = render_raw(
        &loaded.model,
        &loaded.span,
        &buffer,
        &loaded.dialect,
        by_bytes,
    )
    .expect("the raw text renders");

    assert_eq!(rendered.text, "id,city\n0,Lima\n1,Lima\n2,Lima\n");
    assert_eq!(rendered.layout.end, TextEnd::Capped { shown: 3 });

    let aligned = render_aligned(&loaded.model, &buffer, by_lines);
    assert_eq!(aligned.layout.end, TextEnd::Capped { shown: 10 });
    assert_eq!(aligned.text.lines().count(), 11);
}

#[test]
fn records_whose_bytes_were_not_kept_end_the_raw_text() {
    let bytes = b"id\n0\n1\n2\n3\n";
    let mut loaded = load_whole(bytes, dialect(true, utf_8()));

    // Keep the header and the first two records only.
    loaded.span = SourceSpan::new(0, b"id\n0\n1\n".to_vec());

    let rendered = loaded.raw(&loaded.buffer());

    assert_eq!(rendered.text, "id\n0\n1\n");
    assert_eq!(rendered.layout.end, TextEnd::Capped { shown: 2 });
}

#[test]
fn the_kept_bytes_grow_only_with_the_records_that_follow_them() {
    let mut span = SourceSpan::new(3, b"ab\n".to_vec());

    span.extend(6, Some(b"cd\n".to_vec()));
    assert_eq!(span.end(), 9);
    assert_eq!(span.record(&(6..9)), Some(&b"cd\n"[..]));
    assert_eq!(span.record(&(3..6)), Some(&b"ab\n"[..]));
    assert_eq!(span.record(&(9..12)), None);
    assert_eq!(span.budget(100), 94);
    assert_eq!(span.budget(6), 0);

    // A gap closes it: nothing after the gap is ever kept.
    span.extend(12, Some(b"ef\n".to_vec()));
    assert_eq!(span.end(), 9);
    assert_eq!(span.budget(100), 0);

    span.extend(9, Some(b"gh\n".to_vec()));
    assert_eq!(span.end(), 9);

    let mut skipped = SourceSpan::new(0, Vec::new());
    skipped.extend(0, None);
    assert_eq!(skipped.budget(100), 0);
}

#[test]
fn a_page_whose_bytes_were_kept_only_in_part_closes_the_span() {
    let loaded = load(b"id\n0\n11\n222\n3333\n", dialect(true, utf_8()), 2, 2);
    let records = loaded.model.records();

    let mut whole = SourceSpan::new(0, b"id\n0\n".to_vec());
    whole.extend_with_records(&records[1..3], Some(b"11\n222\n".to_vec()));
    assert_eq!(whole.end(), 12);
    assert!(whole.budget(100) > 0);

    let mut part = SourceSpan::new(0, b"id\n0\n".to_vec());
    part.extend_with_records(&records[1..3], Some(b"11\n".to_vec()));
    assert_eq!(part.end(), 8);
    assert_eq!(part.budget(100), 0);

    let mut empty_page = SourceSpan::new(0, b"id\n".to_vec());
    empty_page.extend_with_records(&[], None);
    assert!(empty_page.budget(100) > 0);
}

// -- Aligned ----------------------------------------------------------------------

#[test]
fn the_aligned_text_pads_every_column_to_its_widest_value() {
    let loaded = load_whole(
        b"name,city\nAna,Lima\nBartholomew,Rome\n",
        dialect(true, utf_8()),
    );

    let rendered = render_aligned(&loaded.model, &loaded.buffer(), NO_LIMITS);

    assert_eq!(
        rendered.text,
        "name        | city\nAna         | Lima\nBartholomew | Rome\n"
    );
    assert_eq!(rendered.layout.end, TextEnd::Complete);
    assert_eq!(rendered.layout.line_of(TextRow::Base(1)), Some(2));
}

#[test]
fn the_aligned_text_pads_a_short_record_with_empty_cells() {
    let loaded = load_whole(b"1,2,3\n4\n5,6\n", dialect(false, utf_8()));

    let rendered = render_aligned(&loaded.model, &loaded.buffer(), NO_LIMITS);

    assert_eq!(
        rendered.text,
        "column_1 | column_2 | column_3\n1        | 2        | 3\n4        |          |\n5        | 6        |\n"
    );
}

#[test]
fn the_aligned_text_shows_a_line_break_in_a_value_as_a_mark() {
    let loaded = load_whole(
        b"a,b\n\"one\r\ntwo\nthree\",\"x\ty\"\n",
        dialect(true, utf_8()),
    );

    let rendered = render_aligned(&loaded.model, &loaded.buffer(), NO_LIMITS);

    let header = format!("{:13} | b", "a");

    assert_eq!(
        rendered.text,
        format!("{header}\none\u{21b5}two\u{21b5}three | x\u{21e5}y\n")
    );
    assert_eq!(rendered.text.lines().count(), 2);
}

#[test]
fn the_aligned_text_cuts_a_wide_value() {
    let wide = "w".repeat(ALIGNED_MAX_WIDTH + 25);
    let bytes = format!("a,b\n{wide},end\n");
    let loaded = load_whole(bytes.as_bytes(), dialect(true, utf_8()));

    let rendered = render_aligned(&loaded.model, &loaded.buffer(), NO_LIMITS);

    let cut = format!("{}\u{2026}", "w".repeat(ALIGNED_MAX_WIDTH - 1));
    let header = format!("{:width$} | b", "a", width = ALIGNED_MAX_WIDTH);

    assert_eq!(rendered.text, format!("{header}\n{cut} | end\n"));
}

#[test]
fn the_aligned_text_shows_pending_changes_and_leaves_deleted_rows_out() {
    let mut loaded = load_whole(b"a,b\n1,2\n3,4\n", dialect(true, utf_8()));
    loaded
        .model
        .rename_column(0, "first".to_string())
        .expect("a file with a header renames");

    let mut buffer = loaded.buffer();
    stage(&mut buffer, 0, 1, "twenty");
    buffer.mark_for_delete(1);
    buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_row(&["0", "zero"]));

    let rendered = render_aligned(&loaded.model, &buffer, NO_LIMITS);

    assert_eq!(rendered.text, "first | b\n0     | zero\n1     | twenty\n");

    let rows: Vec<TextRow> = rendered
        .layout
        .records
        .iter()
        .map(|record| record.row)
        .collect();
    assert_eq!(
        rows,
        [TextRow::Header, TextRow::Insert(0), TextRow::Base(0)]
    );
}

#[test]
fn a_partly_loaded_file_says_more_follows_in_the_aligned_text_too() {
    let loaded = load(b"id\n0\n1\n2\n", dialect(true, utf_8()), 2, 1);

    let rendered = render_aligned(&loaded.model, &loaded.buffer(), NO_LIMITS);

    assert_eq!(rendered.text, "id\n0\n1\n");
    assert_eq!(rendered.layout.end, TextEnd::MoreInFile);
}

#[test]
fn a_line_longer_than_the_wrap_limit_makes_the_text_wrap() {
    let long = "x".repeat(super::text::WRAP_LINE_BYTES + 1);
    let bytes = format!("a\n{long}\n");
    let loaded = load_whole(bytes.as_bytes(), dialect(true, utf_8()));

    assert!(loaded.raw(&loaded.buffer()).layout.wraps);
    assert!(
        !render_aligned(&loaded.model, &loaded.buffer(), NO_LIMITS)
            .layout
            .wraps
    );

    let short = load_whole(b"a\nb\n", dialect(true, utf_8()));
    assert!(!short.raw(&short.buffer()).layout.wraps);
}

// -- The header and the refusals of a save --------------------------------------

#[test]
fn a_header_whose_bytes_were_not_kept_shows_no_text_and_says_so() {
    let mut loaded = load_whole(b"name", dialect(true, utf_8()));

    let mut span = SourceSpan::new(0, Vec::new());
    span.close();
    loaded.span = span;

    let rendered = loaded.raw(&loaded.buffer());
    assert_eq!(rendered.text, "");
    assert_eq!(rendered.layout.end, TextEnd::Capped { shown: 0 });

    let mut buffer = loaded.buffer();
    buffer.add_pending_insert_at(InsertAnchor::End, text_row(&["n,m"]));

    let rendered = loaded.raw(&buffer);
    assert_eq!(rendered.text, "");
    assert_eq!(rendered.layout.end, TextEnd::Capped { shown: 0 });
}

#[test]
fn a_header_past_the_limits_shows_no_text() {
    let loaded = load_whole(b"name,city\n1,2\n", dialect(true, utf_8()));
    let buffer = loaded.buffer();

    for limits in [
        TextLimits {
            max_bytes: 3,
            max_lines: usize::MAX,
        },
        TextLimits {
            max_bytes: usize::MAX,
            max_lines: 0,
        },
    ] {
        let raw = render_raw(
            &loaded.model,
            &loaded.span,
            &buffer,
            &loaded.dialect,
            limits,
        )
        .expect("the raw text renders");
        assert_eq!(raw.text, "", "{limits:?}");
        assert_eq!(raw.layout.end, TextEnd::Capped { shown: 0 }, "{limits:?}");
        assert_tiles(&raw);

        let aligned = render_aligned(&loaded.model, &buffer, limits);
        assert_eq!(aligned.text, "", "{limits:?}");
        assert_eq!(
            aligned.layout.end,
            TextEnd::Capped { shown: 0 },
            "{limits:?}"
        );
    }
}

#[test]
fn a_row_inserted_after_a_header_without_a_terminator_gives_the_header_one() {
    let loaded = load_whole(b"name,city", dialect(true, utf_8()));

    let mut buffer = loaded.buffer();
    buffer.add_pending_insert_at(InsertAnchor::End, text_row(&["n", "m"]));

    let rendered = loaded.raw(&buffer);

    assert_eq!(rendered.text, loaded.saved_text(&buffer));
    assert_eq!(rendered.text, "name,city\nn,m\n");
}

#[test]
fn pending_changes_a_save_would_refuse_show_its_refusal_instead_of_text() {
    let utf_16le = Encoding::for_label(b"utf-16le").expect("a known encoding label");

    // UTF-8 bytes read as UTF-16: an odd length ends inside a code unit.
    let loaded = load_whole(b"a,b\n1,2\nxyz", dialect(false, utf_16le));
    assert_eq!(loaded.bytes.len() % 2, 1);

    let untouched = loaded.raw(&loaded.buffer());
    assert_eq!(untouched.text, decode(&loaded.bytes, &loaded.dialect).text);

    let mut buffer = loaded.buffer();
    buffer.add_pending_insert_at(InsertAnchor::BeforeFirst, text_row(&["new"]));

    let error = render_raw(
        &loaded.model,
        &loaded.span,
        &buffer,
        &loaded.dialect,
        NO_LIMITS,
    )
    .expect_err("a save refuses this");

    assert!(
        error.to_string().contains("in the middle of a character"),
        "{error}"
    );
}
