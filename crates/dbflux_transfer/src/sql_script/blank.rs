//! One blanking pass shared by every keyword detector (transaction control,
//! routine bodies, sequence calls, psql-only content), so none of them reads
//! a word that sits in a string, a quoted identifier, a comment or a
//! dollar-quoted body.

use super::directives::{closes_dollar, dollar_tag};

/// What happens to double-quoted and backtick-quoted identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Identifiers {
    /// Blanked like strings: a column called `"begin"` is not a keyword.
    Blank,
    /// Kept without their quotes, with anything but letters, digits and `_`
    /// blanked, so `"setval"(` reads as a call to `setval` while `"setval("`
    /// does not.
    Keep,
}

fn keep_identifier_char(character: char) -> char {
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

/// `statement` with quoted text, comments and
/// dollar-quoted bodies replaced by spaces. Newlines stay, so offsets keep
/// their line numbers.
pub(super) fn blank_literals(statement: &str, identifiers: Identifiers) -> String {
    let characters: Vec<char> = statement.chars().collect();
    let mut output = String::with_capacity(statement.len());
    let mut position = 0;

    while let Some(&current) = characters.get(position) {
        let next = characters.get(position + 1).copied();
        let end = match (current, next) {
            ('-', Some('-')) => position + line_comment_len(&characters, position),
            ('/', Some('*')) => block_comment_end(&characters, position),
            ('"' | '`', _) if identifiers == Identifiers::Keep => {
                output.push(' ');
                let end = quoted_end(&characters, position, current);
                output.extend(
                    characters
                        .iter()
                        .skip(position + 1)
                        .take(end.saturating_sub(position + 2))
                        .map(|c| keep_identifier_char(*c)),
                );
                output.push(' ');
                position = end;
                continue;
            }
            ('\'' | '"' | '`', _) => quoted_end(&characters, position, current),
            ('$', _) => dollar_end(&characters, position),
            _ => position,
        };

        if end > position {
            output.extend(
                characters
                    .iter()
                    .skip(position)
                    .take(end - position)
                    .map(|c| if *c == '\n' { '\n' } else { ' ' }),
            );
            position = end;
        } else {
            output.push(current);
            position += 1;
        }
    }

    output
}

fn line_comment_len(characters: &[char], start: usize) -> usize {
    characters
        .iter()
        .skip(start)
        .position(|c| *c == '\n')
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
