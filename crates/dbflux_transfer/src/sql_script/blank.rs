//! One blanking pass shared by every keyword detector (transaction control,
//! routine bodies, sequence calls, psql-only content), so none of them reads
//! a word that sits in a string, a quoted identifier, a comment or a
//! dollar-quoted body.

use super::directives::{closes_dollar, dollar_tag};

/// What happens to quoted identifiers and, for one mode, strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Identifiers {
    /// Blanked like strings: a column called `"begin"` is not a keyword.
    Blank,
    /// Kept without their quotes, with anything but letters, digits and `_`
    /// blanked, so `"setval"(` reads as a call to `setval` while `"setval("`
    /// does not.
    Keep,
    /// As [`Identifiers::Keep`], and single-quoted strings are kept too, with
    /// everything but letters, digits and `_` turned into `_`, so a value
    /// such as `'OFF'` stays one word while `'autocommit 0'` cannot pass for
    /// two.
    KeepStrings,
}

fn keep_identifier_character(character: char) -> char {
    if character.is_alphanumeric() || character == '_' {
        character
    } else if character == '\n' {
        '\n'
    } else {
        ' '
    }
}

/// Upper-cased words with their byte offsets.
pub(super) fn words_with_offsets(code: &str) -> Vec<(usize, String)> {
    let mut words = Vec::new();
    let mut start: Option<usize> = None;

    for (offset, character) in code.char_indices().chain([(code.len(), ' ')]) {
        let in_word = character.is_alphanumeric() || character == '_';
        match (start, in_word) {
            (None, true) => start = Some(offset),
            (Some(begin), false) => {
                if let Some(word) = code.get(begin..offset) {
                    words.push((begin, word.to_ascii_uppercase()));
                }
                start = None;
            }
            _ => {}
        }
    }

    words
}

/// `statement` with quoted text, comments and dollar-quoted bodies replaced
/// by spaces. Newlines stay, so offsets keep their line numbers. The body of
/// a MySQL conditional comment (`/*!50003 ... */`) is code, so it is kept.
pub(super) fn blank_literals(statement: &str, identifiers: Identifiers) -> String {
    let characters: Vec<char> = statement.chars().collect();
    let mut output = String::with_capacity(statement.len());
    let mut position = 0;
    let mut conditional_depth = 0usize;

    while let Some(&current) = characters.get(position) {
        let next = characters.get(position + 1).copied();
        let previous = position
            .checked_sub(1)
            .and_then(|before| characters.get(before))
            .copied();

        if (current, next) == ('/', Some('*')) {
            let marker = conditional_marker_len(&characters, position);
            if marker > 0 {
                output.extend(std::iter::repeat_n(' ', marker));
                position += marker;
                conditional_depth += 1;
                continue;
            }
        }

        if conditional_depth > 0 && (current, next) == ('*', Some('/')) {
            output.push_str("  ");
            position += 2;
            conditional_depth -= 1;
            continue;
        }

        let keeps_quoted = match current {
            '"' | '`' => identifiers != Identifiers::Blank,
            '\'' => identifiers == Identifiers::KeepStrings,
            _ => false,
        };
        if keeps_quoted {
            let end = quoted_end(&characters, position, current);
            let keep_character = if current == '\'' {
                keep_string_character
            } else {
                keep_identifier_character
            };
            output.push(' ');
            output.extend(
                characters
                    .iter()
                    .skip(position + 1)
                    .take(end.saturating_sub(position + 2))
                    .map(|character| keep_character(*character)),
            );
            output.push(' ');
            position = end;
            continue;
        }

        let starts_dollar_quote = current == '$'
            && !previous
                .is_some_and(|before| before.is_alphanumeric() || before == '_' || before == '$');

        let end = match (current, next) {
            ('-', Some('-')) => position + line_comment_len(&characters, position),
            ('/', Some('*')) => block_comment_end(&characters, position),
            ('\'' | '"' | '`', _) => quoted_end(&characters, position, current),
            ('$', _) if starts_dollar_quote => dollar_end(&characters, position),
            _ => position,
        };

        if end > position {
            output.extend(
                characters
                    .iter()
                    .skip(position)
                    .take(end - position)
                    .map(|character| if *character == '\n' { '\n' } else { ' ' }),
            );
            position = end;
        } else {
            output.push(current);
            position += 1;
        }
    }

    output
}

/// Length of the opening of a conditional comment at `start` (`/*!50003`,
/// `/*M!100100`), or 0 when the comment there is an ordinary one.
fn conditional_marker_len(characters: &[char], start: usize) -> usize {
    let after_open = start + 2;
    let marker = match characters.get(after_open) {
        Some('!') => 1,
        Some('M') if characters.get(after_open + 1) == Some(&'!') => 2,
        _ => return 0,
    };
    let digits = characters
        .iter()
        .skip(after_open + marker)
        .take_while(|character| character.is_ascii_digit())
        .count();

    2 + marker + digits
}

fn keep_string_character(character: char) -> char {
    if character.is_alphanumeric() || character == '_' {
        character
    } else if character == '\n' {
        '\n'
    } else {
        '_'
    }
}

fn line_comment_len(characters: &[char], start: usize) -> usize {
    characters
        .iter()
        .skip(start)
        .position(|character| *character == '\n')
        .unwrap_or(characters.len() - start)
}

fn block_comment_end(characters: &[char], start: usize) -> usize {
    let mut depth = 0usize;
    let mut position = start;

    while position < characters.len() {
        let pair = (
            characters.get(position).copied(),
            characters.get(position + 1).copied(),
        );
        match pair {
            (Some('/'), Some('*')) => {
                depth += 1;
                position += 2;
            }
            (Some('*'), Some('/')) => {
                depth = depth.saturating_sub(1);
                position += 2;
                if depth == 0 {
                    return position;
                }
            }
            _ => position += 1,
        }
    }

    characters.len()
}

/// End (exclusive) of the quoted run opened by `quote` at `start`; a doubled
/// quote is an escape.
fn quoted_end(characters: &[char], start: usize, quote: char) -> usize {
    let mut position = start + 1;

    while let Some(&current) = characters.get(position) {
        position += 1;
        if current == quote {
            if characters.get(position) == Some(&quote) {
                position += 1;
            } else {
                return position;
            }
        }
    }

    characters.len()
}

/// End (exclusive) of a dollar-quoted body opened at `start`, or `start`
/// when this `$` does not open one.
fn dollar_end(characters: &[char], start: usize) -> usize {
    let Some((tag, consumed)) = dollar_tag(characters, start + 1) else {
        return start;
    };
    let mut position = start + 1 + consumed;

    while position < characters.len() {
        if characters.get(position) == Some(&'$') && closes_dollar(characters, position + 1, &tag) {
            return position + 1 + tag.chars().count() + 1;
        }
        position += 1;
    }

    characters.len()
}
