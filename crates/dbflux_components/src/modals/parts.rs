//! The product half of the dialog body blocks: which words a code preview
//! highlights. The rendering itself lives in `bolt::modals::parts`.

use std::ops::Range;

use gpui::{App, Div, SharedString};

pub use bolt::modals::parts::{
    inline_code, modal_field, modal_form_row, modal_frame, modal_hint, modal_lead,
    modal_value_field,
};

/// Words drawn as keywords in a [`modal_code`] statement.
const SQL_KEYWORDS: &[&str] = &[
    "ADD", "ALTER", "AND", "AS", "BY", "CASCADE", "COLUMN", "CREATE", "DATABASE", "DELETE", "DROP",
    "EXISTS", "FROM", "GROUP", "IF", "IN", "INDEX", "INSERT", "INTO", "IS", "JOIN", "LIMIT", "NOT",
    "NULL", "ON", "OR", "ORDER", "RENAME", "SELECT", "SET", "TABLE", "TO", "TRUNCATE", "UPDATE",
    "VALUES", "VIEW", "WHERE",
];

/// Byte ranges of the SQL keywords in `text`, outside quoted strings and
/// identifiers.
fn sql_keyword_ranges(text: &str) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut quote: Option<char> = None;
    let mut word_start: Option<usize> = None;

    let close_word = |start: Option<usize>, end: usize, ranges: &mut Vec<Range<usize>>| {
        if let Some(start) = start {
            let word = &text[start..end];

            if SQL_KEYWORDS
                .iter()
                .any(|keyword| keyword.eq_ignore_ascii_case(word))
            {
                ranges.push(start..end);
            }
        }
    };

    for (index, character) in text.char_indices() {
        if let Some(open) = quote {
            if character == open {
                quote = None;
            }
            continue;
        }

        if character.is_alphanumeric() || character == '_' {
            if word_start.is_none() {
                word_start = Some(index);
            }
            continue;
        }

        close_word(word_start.take(), index, &mut ranges);

        if matches!(character, '\'' | '"' | '`') {
            quote = Some(character);
        }
    }

    if quote.is_none() {
        close_word(word_start, text.len(), &mut ranges);
    }

    ranges
}

/// A statement block with this product's SQL keywords highlighted.
pub fn modal_code(code: impl Into<SharedString>, cx: &App) -> Div {
    let code: SharedString = code.into();
    let ranges = sql_keyword_ranges(&code);

    bolt::modals::parts::modal_code(code, &ranges, cx)
}

#[cfg(test)]
mod tests {
    use super::sql_keyword_ranges;

    fn keywords(text: &str) -> Vec<&str> {
        sql_keyword_ranges(text)
            .into_iter()
            .map(|range| &text[range])
            .collect()
    }

    #[test]
    fn keywords_are_found_in_any_case() {
        assert_eq!(keywords("DELETE FROM sessions;"), vec!["DELETE", "FROM"]);
        assert_eq!(
            keywords("drop table public.orders cascade;"),
            vec!["drop", "table", "cascade"]
        );
    }

    #[test]
    fn quoted_text_and_identifiers_are_skipped() {
        assert_eq!(
            keywords("UPDATE t SET note = 'delete from' WHERE \"table\" = 1"),
            vec!["UPDATE", "SET", "WHERE"]
        );
    }

    #[test]
    fn words_containing_a_keyword_are_not_keywords() {
        assert!(keywords("deleted_at fromage").is_empty());
    }
}
