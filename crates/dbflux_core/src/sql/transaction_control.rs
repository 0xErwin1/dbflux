/// Strict classification of standalone SQL transaction-control statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionControl {
    Begin,
    Commit,
    Rollback,
    Unsupported,
    NotControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token<'a> {
    Word(&'a str),
    Semicolon,
    Other,
}

/// Classifies one SQL input without validating ordinary SQL syntax.
///
/// Empty and comment-only input deliberately fail closed as [`TransactionControl::Unsupported`].
/// Ordinary SQL is returned as [`TransactionControl::NotControl`] even when it is not otherwise
/// valid SQL; this lexer exists only to protect transaction-control session handling.
pub fn classify_sql_transaction_control(sql: &str) -> TransactionControl {
    let tokens = match lex(sql) {
        Ok(tokens) => tokens,
        Err(()) => return TransactionControl::Unsupported,
    };

    let mut statement_count = 0;
    let mut statement = Vec::new();
    let mut current_statement = Vec::new();

    for token in tokens {
        if token == Token::Semicolon {
            if !current_statement.is_empty() {
                statement_count += 1;
                if statement_count == 1 {
                    statement = current_statement;
                }
                current_statement = Vec::new();
            }
        } else {
            current_statement.push(token);
        }
    }

    if !current_statement.is_empty() {
        statement_count += 1;
        if statement_count == 1 {
            statement = current_statement;
        }
    }

    if statement_count != 1 {
        return TransactionControl::Unsupported;
    }

    classify_statement(&statement)
}

fn classify_statement(tokens: &[Token<'_>]) -> TransactionControl {
    // A quoted or symbolic tail after a transaction-control lead is not ordinary
    // SQL. It must fail closed rather than falling through the generic lexer path.
    if matches!(tokens.first(), Some(Token::Word(first)) if is_transaction_control_lead(first))
        && tokens.iter().any(|token| matches!(token, Token::Other))
    {
        return TransactionControl::Unsupported;
    }

    let words: Option<Vec<&str>> = tokens
        .iter()
        .map(|token| match token {
            Token::Word(word) => Some(*word),
            Token::Semicolon | Token::Other => None,
        })
        .collect();

    let Some(words) = words else {
        return TransactionControl::NotControl;
    };

    match words.as_slice() {
        [begin] if keyword(begin, "BEGIN") => TransactionControl::Begin,
        [begin, immediate] if keyword(begin, "BEGIN") && keyword(immediate, "IMMEDIATE") => {
            TransactionControl::Begin
        }
        [begin, transaction] if keyword(begin, "BEGIN") && keyword(transaction, "TRANSACTION") => {
            TransactionControl::Begin
        }
        [commit] if keyword(commit, "COMMIT") => TransactionControl::Commit,
        [rollback] if keyword(rollback, "ROLLBACK") => TransactionControl::Rollback,
        [first, ..] if is_transaction_control_lead(first) => TransactionControl::Unsupported,
        _ => TransactionControl::NotControl,
    }
}

fn keyword(word: &str, expected: &str) -> bool {
    word.eq_ignore_ascii_case(expected)
}

fn is_transaction_control_lead(word: &str) -> bool {
    [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "START",
        "END",
        "SET",
    ]
    .iter()
    .any(|keyword_name| keyword(word, keyword_name))
}

fn lex(sql: &str) -> Result<Vec<Token<'_>>, ()> {
    let mut tokens = Vec::new();
    let mut position = 0;

    while let Some(character) = char_at(sql, position) {
        if character.is_whitespace() {
            position += character.len_utf8();
        } else if sql[position..].starts_with("--") {
            position += 2;
            while let Some(comment_character) = char_at(sql, position) {
                position += comment_character.len_utf8();
                if comment_character == '\n' || comment_character == '\r' {
                    break;
                }
            }
        } else if sql[position..].starts_with("/*") {
            position = skip_block_comment(sql, position)?;
        } else if character == '\'' || character == '"' || character == '`' || character == '[' {
            position = skip_quoted(sql, position, character)?;
            tokens.push(Token::Other);
        } else if character == '$' {
            if let Some(delimiter) = dollar_quote_delimiter(&sql[position..]) {
                position = skip_dollar_quote(sql, position, delimiter)?;
                tokens.push(Token::Other);
            } else {
                position += character.len_utf8();
                tokens.push(Token::Other);
            }
        } else if character == ';' {
            position += character.len_utf8();
            tokens.push(Token::Semicolon);
        } else if is_identifier_character(character) {
            let start = position;
            position += character.len_utf8();
            while let Some(identifier_character) = char_at(sql, position) {
                if !is_identifier_character(identifier_character) {
                    break;
                }
                position += identifier_character.len_utf8();
            }
            tokens.push(Token::Word(&sql[start..position]));
        } else {
            position += character.len_utf8();
            tokens.push(Token::Other);
        }
    }

    Ok(tokens)
}

fn char_at(sql: &str, position: usize) -> Option<char> {
    sql.get(position..)?.chars().next()
}

fn is_identifier_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

fn skip_block_comment(sql: &str, mut position: usize) -> Result<usize, ()> {
    let mut depth = 0;

    while position < sql.len() {
        if sql[position..].starts_with("/*") {
            depth += 1;
            position += 2;
        } else if sql[position..].starts_with("*/") {
            depth -= 1;
            position += 2;
            if depth == 0 {
                return Ok(position);
            }
        } else if let Some(character) = char_at(sql, position) {
            position += character.len_utf8();
        } else {
            break;
        }
    }

    Err(())
}

fn skip_quoted(sql: &str, mut position: usize, opening: char) -> Result<usize, ()> {
    let closing = if opening == '[' { ']' } else { opening };
    position += opening.len_utf8();

    while let Some(character) = char_at(sql, position) {
        if character == '\\' {
            position += character.len_utf8();
            let Some(escaped) = char_at(sql, position) else {
                return Err(());
            };
            position += escaped.len_utf8();
        } else if character == closing {
            position += character.len_utf8();
            if char_at(sql, position) == Some(closing) {
                position += closing.len_utf8();
            } else {
                return Ok(position);
            }
        } else {
            position += character.len_utf8();
        }
    }

    Err(())
}

fn dollar_quote_delimiter(sql: &str) -> Option<&str> {
    if !sql.starts_with('$') {
        return None;
    }

    let delimiter_end = sql[1..].find('$')? + 2;
    let tag = &sql[1..delimiter_end - 1];
    if tag.chars().enumerate().all(|(index, character)| {
        if index == 0 {
            character.is_ascii_alphabetic() || character == '_'
        } else {
            is_identifier_character(character)
        }
    }) && (tag.is_empty()
        || tag
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_'))
    {
        Some(&sql[..delimiter_end])
    } else {
        None
    }
}

fn skip_dollar_quote(sql: &str, position: usize, delimiter: &str) -> Result<usize, ()> {
    let body_start = position + delimiter.len();
    let remaining = sql.get(body_start..).ok_or(())?;
    let body_end = remaining.find(delimiter).ok_or(())?;
    Ok(body_start + body_end + delimiter.len())
}

#[cfg(test)]
mod tests {
    use super::{TransactionControl, classify_sql_transaction_control};

    #[test]
    fn accepts_standalone_transaction_controls() {
        for (sql, expected) in [
            ("BEGIN", TransactionControl::Begin),
            ("begin immediate", TransactionControl::Begin),
            ("BEGIN TRANSACTION", TransactionControl::Begin),
            (
                "/* lead */ BEGIN /* between */ IMMEDIATE; -- trailing",
                TransactionControl::Begin,
            ),
            ("COMMIT; ; -- trailing", TransactionControl::Commit),
            ("ROLLBACK", TransactionControl::Rollback),
        ] {
            assert_eq!(classify_sql_transaction_control(sql), expected, "{sql}");
        }
    }

    #[test]
    fn rejects_unsupported_or_malformed_transaction_controls() {
        for sql in [
            "BEGIN DEFERRED",
            "BEGIN EXCLUSIVE",
            "BEGIN WORK",
            "COMMIT TRANSACTION",
            "ROLLBACK TO savepoint_name",
            "SAVEPOINT savepoint_name",
            "RELEASE savepoint_name",
            "START TRANSACTION",
            "END",
            "SET TRANSACTION READ ONLY",
            "BEGIN; COMMIT",
            "SELECT 1; SELECT 2",
            "BEGIN trailing",
            "BEGIN 'quoted tail'",
            "BEGIN $tag$quoted tail$tag$",
            "BEGIN 'unterminated",
            "/* unterminated",
            "/* nested /* still unterminated */",
            "'unterminated",
            "\"unterminated",
            "`unterminated",
            "[unterminated",
            "$tag$unterminated",
        ] {
            assert_eq!(
                classify_sql_transaction_control(sql),
                TransactionControl::Unsupported,
                "{sql}"
            );
        }
    }

    #[test]
    fn does_not_mistake_ordinary_sql_for_control() {
        for sql in [
            "SELECT 'BEGIN; COMMIT'",
            "SELECT 'it''s; BEGIN'",
            "SELECT 'escaped \\' ; BEGIN'",
            "SELECT $$BEGIN; COMMIT$$",
            "SELECT $tag$BEGIN; COMMIT$tag$",
            "SELECT \"BE\"\"GIN\" FROM [COM]]MIT]",
            "SELECT `ROLLBACK` FROM values_table",
            "SELECT '中; BEGIN' -- COMMIT\nFROM records",
            "BEGINNING",
            "COMMITTED",
            "ROLLBACKING",
            "BE/* split */GIN",
            "SELECT 1; -- a trailing comment",
        ] {
            assert_eq!(
                classify_sql_transaction_control(sql),
                TransactionControl::NotControl,
                "{sql}"
            );
        }
    }

    #[test]
    fn empty_or_comment_only_input_fails_closed() {
        for sql in ["", " ; ; ", "-- comment only", "/* comment only */"] {
            assert_eq!(
                classify_sql_transaction_control(sql),
                TransactionControl::Unsupported,
                "{sql}"
            );
        }
    }
}
