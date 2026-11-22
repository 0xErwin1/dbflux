use super::*;

#[test]
fn char_position_is_one_based() {
    assert_eq!(char_position_to_byte_offset("select", 1), Some(0));
    assert_eq!(char_position_to_byte_offset("select", 3), Some(2));
}

#[test]
fn char_position_counts_characters_not_bytes() {
    // "é" and "日" take 2 and 3 bytes, so the 4th character starts at byte 6.
    let text = "é日 x";
    assert_eq!(char_position_to_byte_offset(text, 3), Some(5));
    assert_eq!(char_position_to_byte_offset(text, 4), Some(6));
    assert_eq!(&text[6..], "x");
}

#[test]
fn char_position_one_past_the_end_is_the_end_of_the_input() {
    assert_eq!(char_position_to_byte_offset("é", 2), Some(2));
    assert_eq!(char_position_to_byte_offset("", 1), Some(0));
}

#[test]
fn char_position_outside_the_text_is_none() {
    assert_eq!(char_position_to_byte_offset("abc", 0), None);
    assert_eq!(char_position_to_byte_offset("abc", 5), None);
}

#[test]
fn line_column_counts_from_zero() {
    assert_eq!(byte_offset_to_line_column("select 1", 0), Some((0, 0)));
    assert_eq!(byte_offset_to_line_column("select 1", 7), Some((0, 7)));
}

#[test]
fn line_column_follows_newlines() {
    let text = "select 1\nfrom t\nwhere x";
    assert_eq!(byte_offset_to_line_column(text, 9), Some((1, 0)));
    assert_eq!(byte_offset_to_line_column(text, 17), Some((2, 1)));
}

#[test]
fn line_column_counts_characters_in_utf8_lines() {
    let text = "select 'é日'\nfrom t";
    // `'é日'` spans bytes 7..14; the closing quote is the 11th character.
    assert_eq!(byte_offset_to_line_column(text, 13), Some((0, 10)));
    assert_eq!(byte_offset_to_line_column(text, 15), Some((1, 0)));
}

#[test]
fn line_column_treats_crlf_as_one_line_break() {
    let text = "select 1\r\nfrom t\r\nwhere";
    // Byte 10 is the `f` after the CRLF, byte 18 the `w` after the second.
    assert_eq!(byte_offset_to_line_column(text, 10), Some((1, 0)));
    assert_eq!(byte_offset_to_line_column(text, 18), Some((2, 0)));
    // The `\r` is the last column of its own line, not a line of its own.
    assert_eq!(byte_offset_to_line_column(text, 8), Some((0, 8)));
}

#[test]
fn line_column_rejects_offsets_off_the_text() {
    assert_eq!(byte_offset_to_line_column("é", 1), None);
    assert_eq!(byte_offset_to_line_column("abc", 4), None);
    assert_eq!(byte_offset_to_line_column("abc", 3), Some((0, 3)));
}

#[test]
fn locator_finds_statements_in_order_even_when_they_repeat() {
    let request = "SELECT 1;\n  SELECT 1;\n  SELECT 2";
    let mut locator = StatementLocator::new(request);

    assert_eq!(locator.locate("SELECT 1;"), Some(0));
    assert_eq!(locator.locate("SELECT 1;"), Some(12));
    assert_eq!(locator.locate("SELECT 2"), Some(24));
    assert_eq!(locator.locate("SELECT 3"), None);
}

#[test]
fn placing_an_error_shifts_it_or_drops_the_position() {
    use crate::{DbError, FormattedError};

    let error = || DbError::SyntaxError(FormattedError::new("boom").with_position(3));

    let shifted = StatementLocator::place(error(), Some(10));
    assert_eq!(shifted.formatted().and_then(|f| f.position), Some(13));

    let dropped = StatementLocator::place(error(), None);
    assert_eq!(dropped.formatted().and_then(|f| f.position), None);
}
