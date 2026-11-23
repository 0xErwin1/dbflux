//! Detects a compound `BEGIN ... END` body that the `;` splitter cut apart.
//!
//! A MySQL or SQLite routine or trigger body holds `;`-terminated statements.
//! Without a `DELIMITER` directive the splitter ends a statement at the first
//! inner `;`, so a fragment of the body would run on its own. PostgreSQL
//! `$$` bodies are a single quoted token and are not affected.

use super::ScriptStatement;
use super::blank::{Identifiers, blank_literals, words_with_offsets};
use super::control::begin_is_transaction;

/// Line of the first compound body that is still open at the end of its
/// statement, so the splitter cut it.
pub(super) fn split_routine_body_line(statements: &[ScriptStatement]) -> Option<usize> {
    statements.iter().find_map(|statement| {
        let code = blank_literals(&statement.text, Identifiers::Blank);
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
                let after = words.get(position + 2).map(|(_, after)| after.as_str());
                let is_transaction = position == 0 && begin_is_transaction(next, after);
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

/// Whether the statement changes a sequence: it calls `setval(` or
/// `nextval(` (outside quotes and comments), or is `ALTER SEQUENCE`. The
/// shared classifier rates `SELECT setval(...)` as a read, but it moves the
/// sequence, so a script summary and a read-only profile must treat it as a
/// write.
pub(crate) fn changes_a_sequence(statement: &str) -> bool {
    let code = blank_literals(statement, Identifiers::Keep);
    let words = words_with_offsets(&code);

    let alters = matches!(
        (words.first(), words.get(1)),
        (Some((_, first)), Some((_, second))) if first == "ALTER" && second == "SEQUENCE"
    );

    alters
        || words.iter().any(|(offset, word)| {
            matches!(word.as_str(), "SETVAL" | "NEXTVAL")
                && code
                    .get(offset + word.len()..)
                    .is_some_and(|rest| rest.trim_start().starts_with('('))
        })
}
