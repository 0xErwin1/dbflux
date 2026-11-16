use std::fs;
use std::path::Path;

use dbflux_core::QueryLanguage;

use super::directives::find_directive;
use super::routine::split_routine_body_line;
use super::{ScriptError, ScriptStatement, SqlScript};

/// Scripts larger than this are refused before they are read.
pub const MAX_SCRIPT_BYTES: u64 = 100 * 1024 * 1024;

/// Reads the script at `path` and splits it into statements for `language`.
///
/// Refuses, with an error that says what to do next: a file over
/// [`MAX_SCRIPT_BYTES`] (the error carries its size), a file that cannot be
/// read, text that is not UTF-8 (the error carries the byte position), a
/// language that is not SQL, and a script with no statements (empty, or only
/// comments).
pub fn load_script(path: &Path, language: &QueryLanguage) -> Result<SqlScript, ScriptError> {
    let unreadable = |error: std::io::Error| ScriptError::Unreadable {
        path: path.display().to_string(),
        message: error.to_string(),
    };

    let size = fs::metadata(path).map_err(unreadable)?.len();
    if size > MAX_SCRIPT_BYTES {
        return Err(ScriptError::TooLarge {
            size,
            limit: MAX_SCRIPT_BYTES,
        });
    }

    let bytes = fs::read(path).map_err(unreadable)?;
    let text = String::from_utf8(bytes).map_err(|error| ScriptError::NotUtf8 {
        position: error.utf8_error().valid_up_to() + 1,
    })?;

    SqlScript::from_text(&text, language)
}

impl SqlScript {
    /// Splits already-read script text with the editor's splitter
    /// ([`QueryLanguage::statement_ranges`]), so quotes, comments and
    /// dollar-quoted bodies are respected.
    ///
    /// Each statement starts at its first token: comments in front of it are
    /// skipped, and a segment that holds only comments is not a statement.
    /// A MySQL conditional comment (`/*! ... */`) is executable, so it counts
    /// as a statement. A leading byte order mark is ignored.
    pub fn from_text(text: &str, language: &QueryLanguage) -> Result<Self, ScriptError> {
        if !matches!(language, QueryLanguage::Sql) {
            return Err(ScriptError::NotSql {
                language: language_label(language),
            });
        }

        let bytes = text.len() as u64;
        let text = text.strip_prefix('\u{feff}').unwrap_or(text);

        if let Some(directive) = find_directive(text) {
            return Err(ScriptError::ClientDirective {
                directive: directive.name.to_string(),
                line: directive.line,
            });
        }

        let ranges = language.statement_ranges(text).unwrap_or_default();

        let mut statements = Vec::with_capacity(ranges.len());
        let mut line = 1;
        let mut scanned = 0;

        for range in ranges {
            let Some(segment) = text.get(range.clone()) else {
                continue;
            };
            let first_token = range.start + skip_comments(segment);
            if first_token >= range.end {
                continue;
            }

            line += text[scanned..first_token].matches('\n').count();
            scanned = first_token;
            statements.push(ScriptStatement {
                text: text[first_token..range.end].to_string(),
                line,
            });
        }

        if statements.is_empty() {
            return Err(ScriptError::Empty);
        }

        if let Some(line) = split_routine_body_line(&statements) {
            return Err(ScriptError::SplitRoutineBody { line });
        }

        Ok(Self::new(statements, bytes))
    }
}

fn language_label(language: &QueryLanguage) -> String {
    match language {
        QueryLanguage::MongoQuery => "MongoDB".to_string(),
        QueryLanguage::RedisCommands => "Redis".to_string(),
        other => format!("{other:?}"),
    }
}

/// Byte offset of the first token of `text`, past whitespace, `--` line
/// comments and nestable `/* */` block comments. `/*!` conditional comments
/// are tokens. Equals `text.len()` when nothing but comments is left.
fn skip_comments(text: &str) -> usize {
    let mut rest = text;

    loop {
        let trimmed = rest.trim_start();

        if let Some(after) = trimmed.strip_prefix("--") {
            rest = after.split_once('\n').map_or("", |(_, tail)| tail);
        } else if trimmed.starts_with("/*") && !trimmed.starts_with("/*!") {
            rest = after_block_comment(trimmed);
        } else {
            return text.len() - trimmed.len();
        }
    }
}

/// `text` (which starts with `/*`) without its leading block comment; empty
/// when the comment is never closed.
fn after_block_comment(text: &str) -> &str {
    let mut depth = 0usize;
    let mut rest = text;

    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix("/*") {
            depth += 1;
            rest = after;
        } else if let Some(after) = rest.strip_prefix("*/") {
            depth = depth.saturating_sub(1);
            rest = after;
            if depth == 0 {
                return rest;
            }
        } else {
            let mut chars = rest.chars();
            chars.next();
            rest = chars.as_str();
        }
    }

    ""
}
