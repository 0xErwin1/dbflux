#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::borrow::Cow;

use encoding_rs::{UTF_8, UTF_16BE, UTF_16LE, WINDOWS_1252};

use super::{Dialect, DialectOverrides, SampleCoverage, decode, detect_dialect};

fn utf16le_with_bom(text: &str) -> Vec<u8> {
    let mut bytes = vec![0xFF, 0xFE];
    bytes.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
    bytes
}

fn utf16be_with_bom(text: &str) -> Vec<u8> {
    let mut bytes = vec![0xFE, 0xFF];
    bytes.extend(text.encode_utf16().flat_map(u16::to_be_bytes));
    bytes
}

fn dialect_with_encoding(encoding: &'static encoding_rs::Encoding) -> Dialect {
    Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: true,
        encoding,
    }
}

#[test]
fn detects_comma_delimiter() {
    let dialect = detect_dialect(
        b"name,city,country\nalice,paris,fr\nbob,rome,it\n",
        SampleCoverage::Prefix,
        None,
    );

    assert_eq!(dialect.delimiter, b',');
    assert_eq!(dialect.quote, Some(b'"'));
    assert_eq!(dialect.encoding, UTF_8);
}

#[test]
fn detects_tab_delimiter() {
    let dialect = detect_dialect(
        b"name\tcity\nalice\tparis\nbob\trome\n",
        SampleCoverage::Prefix,
        None,
    );

    assert_eq!(dialect.delimiter, b'\t');
}

#[test]
fn detects_semicolon_delimiter() {
    let dialect = detect_dialect(
        b"name;city\nalice;paris\nbob;rome\n",
        SampleCoverage::Prefix,
        None,
    );

    assert_eq!(dialect.delimiter, b';');
}

#[test]
fn detects_pipe_delimiter() {
    let dialect = detect_dialect(
        b"name|city\nalice|paris\nbob|rome\n",
        SampleCoverage::Prefix,
        None,
    );

    assert_eq!(dialect.delimiter, b'|');
}

#[test]
fn prefers_the_delimiter_with_the_most_consistent_field_count() {
    let sample = b"name\tnote\nalice\thello, world\nbob\ta, b, c\ncarol\tplain\n";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b'\t'
    );
}

#[test]
fn ignores_delimiters_inside_quoted_fields() {
    // Counted naively, the comma count varies per record (1, 3, 2) and the pipe wins.
    let sample = b"a|b,c\n\"x,y,z\"|1,2\n\"p,q\"|3,4\n";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b','
    );
}

#[test]
fn keeps_a_quoted_newline_inside_its_record() {
    // Split on every newline, the comma count becomes inconsistent and the semicolon wins.
    let sample = b"a,b;c\n\"1\n1,1,1\",2;3\n\"4\n4\",5;6\n";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b','
    );
}

#[test]
fn treats_a_doubled_quote_as_an_escaped_quote() {
    // If `""` closed the field, the commas between the escaped quotes would be
    // counted, the comma count would vary (1, 2, 1) and the semicolon would win.
    let sample = b"a,b;c\n\"x \"\"q,q\"\" y\",1;2\n\"z\",3;4\n";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b','
    );
}

#[test]
fn detects_the_same_dialect_for_crlf_and_lf() {
    let lf = detect_dialect(b"1\t2\n3\t4\n", SampleCoverage::Prefix, None);
    let crlf = detect_dialect(b"1\t2\r\n3\t4\r\n", SampleCoverage::Prefix, None);

    assert_eq!(lf.delimiter, b'\t');
    assert!(!lf.has_header);
    assert_eq!(lf, crlf);
}

#[test]
fn bare_carriage_return_ends_a_record() {
    // Read as one record, the sample has four semicolons against three commas
    // and the semicolon wins.
    let sample = b"a;b;c;d;e,f\rg,h\ri,j\r";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b','
    );
}

#[test]
fn unbalanced_quote_does_not_swallow_the_sample() {
    let sample = b"len 5\"|name\n1|x\n2|y\n3|z\n";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b'|'
    );
}

#[test]
fn ignores_a_final_record_cut_off_mid_record() {
    // The complete records tie between `;` and `,`. The cut-off record `4;5`
    // (really `4;5,6`) would hand the win to the semicolon if it were counted.
    let sample = b"a;b,c\n1;2,3\n4;5";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b','
    );
}

#[test]
fn ignores_a_final_record_cut_off_inside_a_quoted_field() {
    // The complete records tie between `;` and `,`. Counting the cut-off record
    // would add a third record with one semicolon and hand it the win.
    let sample = b"a;b,c\n1;2,3\n4;\"un;ter";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b','
    );
}

#[test]
fn whole_file_counts_its_unterminated_final_record() {
    // Same bytes as the cut-off case: as a whole file, `4;5` is a real record
    // and gives the semicolon three agreeing records against two for the comma.
    let sample = b"a;b,c\n1;2,3\n4;5";

    let dialect = detect_dialect(sample, SampleCoverage::WholeFile, None);

    assert_eq!(dialect.delimiter, b';');
}

#[test]
fn uses_a_lone_unterminated_record_when_nothing_else_exists() {
    assert_eq!(
        detect_dialect(b"a|b|c", SampleCoverage::Prefix, None).delimiter,
        b'|'
    );
}

#[test]
fn extension_hint_decides_a_single_column_sample() {
    let sample = b"name\nalice\nbob\n";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, Some("tsv")).delimiter,
        b'\t'
    );
    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, Some("csv")).delimiter,
        b','
    );
    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, None).delimiter,
        b','
    );
}

#[test]
fn extension_hint_breaks_a_tie() {
    let sample = b"a,b\tc\nd,e\tf\n";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, Some("tsv")).delimiter,
        b'\t'
    );
    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, Some(".TSV")).delimiter,
        b'\t'
    );
    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, Some("csv")).delimiter,
        b','
    );
}

#[test]
fn extension_hint_does_not_beat_clear_evidence() {
    // The tab is a valid candidate (two of three records have one), but the
    // comma is consistent across all three.
    let sample = b"a,b\tc\nd,e\tf\ng,h\n";

    assert_eq!(
        detect_dialect(sample, SampleCoverage::Prefix, Some("tsv")).delimiter,
        b','
    );
}

#[test]
fn empty_sample_falls_back_to_the_hint_and_reports_a_header() {
    let dialect = detect_dialect(b"", SampleCoverage::Prefix, Some("tsv"));

    assert_eq!(dialect.delimiter, b'\t');
    assert!(dialect.has_header);
    assert_eq!(dialect.encoding, UTF_8);
}

#[test]
fn utf8_bom_decides_the_encoding() {
    let dialect = detect_dialect(b"\xEF\xBB\xBF1;2\n3;4\n", SampleCoverage::Prefix, None);

    assert_eq!(dialect.encoding, UTF_8);
    assert_eq!(dialect.delimiter, b';');
    assert!(!dialect.has_header);
}

#[test]
fn utf16le_bom_decides_the_encoding() {
    let dialect = detect_dialect(
        &utf16le_with_bom("id\tname\n1\tana\n2\tluis\n"),
        SampleCoverage::Prefix,
        None,
    );

    assert_eq!(dialect.encoding, UTF_16LE);
    assert_eq!(dialect.delimiter, b'\t');
    assert!(dialect.has_header);
}

#[test]
fn utf16be_bom_decides_the_encoding() {
    let dialect = detect_dialect(
        &utf16be_with_bom("1|2\n3|4\n"),
        SampleCoverage::Prefix,
        None,
    );

    assert_eq!(dialect.encoding, UTF_16BE);
    assert_eq!(dialect.delimiter, b'|');
    assert!(!dialect.has_header);
}

#[test]
fn valid_utf8_without_bom_is_utf8() {
    let dialect = detect_dialect(
        "nombre;ciudad\nJosé;Málaga\n".as_bytes(),
        SampleCoverage::Prefix,
        None,
    );

    assert_eq!(dialect.encoding, UTF_8);
}

#[test]
fn utf8_sample_cut_inside_a_multibyte_character_is_still_utf8() {
    let mut sample = "nombre;ciudad\nJosé;Málaga\nMarí".as_bytes().to_vec();
    sample.pop();

    assert!(std::str::from_utf8(&sample).is_err());
    assert_eq!(
        detect_dialect(&sample, SampleCoverage::Prefix, None).encoding,
        UTF_8
    );
}

#[test]
fn whole_file_ending_in_a_lone_legacy_byte_is_not_utf8() {
    let sample = b"a;b\nx;caf\xE9";

    let whole = detect_dialect(sample, SampleCoverage::WholeFile, None);
    let prefix = detect_dialect(sample, SampleCoverage::Prefix, None);

    assert_eq!(whole.encoding, WINDOWS_1252);
    assert_eq!(whole.delimiter, b';');
    assert_eq!(prefix.encoding, UTF_8);
}

#[test]
fn windows_1252_sample_goes_through_the_detector() {
    let sample = b"nombre;ciudad\nJos\xE9;M\xE1laga\nMar\xEDa;C\xF3rdoba\nBego\xF1a;Le\xF3n\n";

    let dialect = detect_dialect(sample, SampleCoverage::Prefix, None);

    assert_eq!(dialect.encoding, WINDOWS_1252);
    assert_eq!(dialect.delimiter, b';');
}

#[test]
fn reports_a_header_when_the_first_record_has_no_numeric_field() {
    assert!(
        detect_dialect(
            b"name,age\nalice,30\nbob,41\n",
            SampleCoverage::Prefix,
            None
        )
        .has_header
    );
}

#[test]
fn reports_no_header_when_the_first_record_has_a_numeric_field() {
    assert!(!detect_dialect(b"1,alice\n2,bob\n", SampleCoverage::Prefix, None).has_header);
    assert!(!detect_dialect(b"alice,30.5\nbob,41\n", SampleCoverage::Prefix, None).has_header);
    assert!(
        !detect_dialect(
            b"\"1\",\"alice\"\n\"2\",\"bob\"\n",
            SampleCoverage::Prefix,
            None
        )
        .has_header
    );
}

#[test]
fn reports_a_header_when_every_record_is_text() {
    assert!(detect_dialect(b"alice,paris\nbob,rome\n", SampleCoverage::Prefix, None).has_header);
    assert!(detect_dialect(b"nan,inf\nx,y\n", SampleCoverage::Prefix, None).has_header);
}

fn detected() -> Dialect {
    Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: true,
        encoding: UTF_8,
    }
}

#[test]
fn empty_overrides_keep_the_detected_dialect() {
    assert_eq!(DialectOverrides::default().apply(detected()), detected());
}

#[test]
fn delimiter_override_wins() {
    let overrides = DialectOverrides {
        delimiter: Some(b'\t'),
        ..DialectOverrides::default()
    };

    assert_eq!(
        overrides.apply(detected()),
        Dialect {
            delimiter: b'\t',
            ..detected()
        }
    );
}

#[test]
fn quote_override_wins_including_no_quote() {
    let single = DialectOverrides {
        quote: Some(Some(b'\'')),
        ..DialectOverrides::default()
    };
    let none = DialectOverrides {
        quote: Some(None),
        ..DialectOverrides::default()
    };

    assert_eq!(single.apply(detected()).quote, Some(b'\''));
    assert_eq!(none.apply(detected()).quote, None);
    assert_eq!(none.apply(detected()).delimiter, b',');
}

#[test]
fn header_override_wins() {
    let overrides = DialectOverrides {
        has_header: Some(false),
        ..DialectOverrides::default()
    };

    assert_eq!(
        overrides.apply(detected()),
        Dialect {
            has_header: false,
            ..detected()
        }
    );
}

#[test]
fn encoding_override_wins() {
    let overrides = DialectOverrides {
        encoding: Some(WINDOWS_1252),
        ..DialectOverrides::default()
    };

    assert_eq!(
        overrides.apply(detected()),
        Dialect {
            encoding: WINDOWS_1252,
            ..detected()
        }
    );
}

#[test]
fn decodes_valid_utf8_without_copying_or_replacing() {
    let decoded = decode("a,é\n".as_bytes(), &dialect_with_encoding(UTF_8));

    assert!(matches!(decoded.text, Cow::Borrowed("a,é\n")));
    assert!(!decoded.had_replacements);
}

#[test]
fn decoding_reports_a_replaced_malformed_sequence() {
    let decoded = decode(b"a,\xFFb\n", &dialect_with_encoding(UTF_8));

    assert_eq!(decoded.text, "a,\u{FFFD}b\n");
    assert!(decoded.had_replacements);
}

#[test]
fn decodes_windows_1252() {
    let decoded = decode(b"Jos\xE9;M\xE1laga\n", &dialect_with_encoding(WINDOWS_1252));

    assert_eq!(decoded.text, "José;Málaga\n");
    assert!(!decoded.had_replacements);
}

#[test]
fn decoding_drops_the_byte_order_mark_of_the_dialect_encoding() {
    let utf16_bytes = utf16le_with_bom("a,é\n");

    let utf8 = decode(b"\xEF\xBB\xBFa,b\n", &dialect_with_encoding(UTF_8));
    let utf16 = decode(&utf16_bytes, &dialect_with_encoding(UTF_16LE));

    assert_eq!(utf8.text, "a,b\n");
    assert_eq!(utf16.text, "a,é\n");
    assert!(!utf16.had_replacements);
}

#[test]
fn decoding_does_not_switch_encoding_on_a_foreign_byte_order_mark() {
    let decoded = decode(b"\xFF\xFEab", &dialect_with_encoding(WINDOWS_1252));

    assert_eq!(decoded.text, "ÿþab");
}
