//! Detects a compound `BEGIN ... END` body that the `;` splitter cut apart.
//!
//! A MySQL or SQLite routine or trigger body holds `;`-terminated statements.
//! Without a `DELIMITER` directive the splitter ends a statement at the first
//! inner `;`, so a fragment of the body would run on its own. PostgreSQL
//! `$$` bodies are a single quoted token and are not affected.

use super::ScriptStatement;
use super::control::BEGIN_FOLLOWERS;
use super::directives::{closes_dollar, dollar_tag};

/// Line of the first compound body that is still open at the end of its
/// statement, so the splitter cut it.
pub(super) fn split_routine_body_line(statements: &[ScriptStatement]) -> Option<usize> {
    statements.iter().find_map(|statement| {
        let code = blank_literals(&statement.text);
        open_body_offset(&code).map(|offset| {
            let before = code.get(..offset).unwrap_or_default();
            statement.line + before.matches('\n').count()
        })
    })
}

/// Byte offset of the `BEGIN` of a compound body left unclosed in `code`.
fn open_body_offset(code: &str) -> Option<usize> {
    let words = words_with_offsets(code);
    let mut blocks: Vec<usize> = Vec::new();
    let mut cases = 0usize;

    for (position, (offset, word)) in words.iter().enumerate() {
        let next = words.get(position + 1).map(|(_, next)| next.as_str());

        match word.as_str() {
            "BEGIN" => {
                let is_transaction =
                    position == 0 && next.is_none_or(|next| BEGIN_FOLLOWERS.contains(&next));
                if !is_transaction {
                    blocks.push(*offset);
                }
            }
            "CASE" => cases += 1,
            "END" => {
                if matches!(next, Some("IF" | "LOOP" | "WHILE" | "REPEAT" | "FOR")) {
                    continue;
                }
                if cases > 0 {
                    cases -= 1;
                } else {
                    blocks.pop();
                }
            }
            _ => {}
        }
    }

    blocks.first().copied()
}

/// Upper-cased words with their byte offsets.
fn words_with_offsets(code: &str) -> Vec<(usize, String)> {
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

/// `statement` with quoted text, quoted identifiers, comments and
/// dollar-quoted bodies replaced by spaces. Newlines stay, so offsets keep
/// their line numbers.
fn blank_literals(statement: &str) -> String {
    let characters: Vec<char> = statement.chars().collect();
    let mut output = String::with_capacity(statement.len());
    let mut position = 0;

    while let Some(&current) = characters.get(position) {
        let next = characters.get(position + 1).copied();
        let end = match (current, next) {
            ('-', Some('-')) => position + line_comment_len(&characters, position),
            ('/', Some('*')) => block_comment_end(&characters, position),
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
