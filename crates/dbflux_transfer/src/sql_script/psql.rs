//! Content only the psql command-line client understands: `COPY ... FROM
//! stdin` data blocks and backslash meta-commands. The server never sees
//! them, so running them as statements would fail or load the wrong data.

use super::ScriptError;
use super::ScriptStatement;
use super::directives::find_in_code_lines;
use super::routine::blank_literals;

/// The first psql-only construct in the script, as the error to refuse it
/// with. A `COPY ... FROM stdin` block is checked first, because its data
/// ends with a `\.` line that is also a meta-command.
pub(super) fn find_psql_only(text: &str, statements: &[ScriptStatement]) -> Option<ScriptError> {
    statements
        .iter()
        .find(|statement| is_copy_from_stdin(&statement.text))
        .map(|statement| ScriptError::CopyFromStdin {
            line: statement.line,
        })
        .or_else(|| {
            find_in_code_lines(text, |line, number| {
                let trimmed = line.trim_start();
                trimmed
                    .starts_with('\\')
                    .then(|| ScriptError::PsqlMetaCommand {
                        command: trimmed
                            .split_whitespace()
                            .next()
                            .unwrap_or("\\")
                            .to_string(),
                        line: number,
                    })
            })
        })
}

/// `COPY ... FROM stdin`, with or without options: the data follows the
/// statement in the script, which only psql feeds to the server.
fn is_copy_from_stdin(statement: &str) -> bool {
    let code = blank_literals(statement);
    let words: Vec<&str> = code.split_whitespace().collect();
    let is_word = |word: &str, expected: &str| {
        word.trim_end_matches(|c: char| !c.is_alphanumeric())
            .eq_ignore_ascii_case(expected)
    };

    words.first().is_some_and(|first| is_word(first, "copy"))
        && words
            .windows(2)
            .any(|pair| matches!(pair, [from, stdin] if is_word(from, "from") && is_word(stdin, "stdin")))
}
