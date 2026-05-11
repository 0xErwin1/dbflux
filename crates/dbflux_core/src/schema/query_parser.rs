/// A table reference extracted from a SQL query by the lightweight tokenizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTableRef {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub table: String,
}

/// Extract tables referenced by a SQL query string.
///
/// Scans for FROM/JOIN/UPDATE/INSERT INTO/DELETE FROM clauses using a
/// keyword-driven tokenizer. This is deliberately lightweight — it handles the
/// common relational cases without a full parser:
///
/// - Schema-qualified names: `schema.table` → `TableRef { schema, table }`
/// - Database-qualified names: `db.schema.table` → all three set
/// - Aliases: `FROM users u` — only `users` is extracted
/// - Quoted identifiers: `"schema"."table"`, `` `db`.`table` ``, `[table]`
/// - Line comments (`--`) and block comments (`/* */`) are stripped first
///
/// Not handled (complexity vs. value tradeoff for v1):
/// - CTEs: best-effort — tables inside the outer WITH body are extracted, but
///   nested CTE references within their own definition blocks may be missed
/// - Subqueries: tables inside `(SELECT ...)` sub-expressions are skipped
/// - VALUES-only INSERT statements: no table references in value lists
pub fn extract_referenced_tables(query: &str) -> Vec<QueryTableRef> {
    let cleaned = strip_comments(query);
    let tokens = tokenize(&cleaned);
    parse_table_refs(&tokens)
}

/// Strip SQL line comments (`--`) and block comments (`/* */`).
fn strip_comments(input: &str) -> String {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut i = 0;

    while i < len {
        if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            // Skip to end of line.
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
        } else if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i += 2; // consume '*/'
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }

    out
}

/// A minimal SQL token for table-reference extraction.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    /// A bare or quoted identifier.
    Ident(String),
    /// A dot separator between qualifier parts.
    Dot,
    /// An opening parenthesis — signals a subquery or function call.
    LParen,
    /// A closing parenthesis — decrements subquery depth.
    RParen,
    /// Other punctuation / whitespace that we do not need.
    Other,
}

/// Tokenize a comment-stripped SQL string into the minimal token stream needed
/// for table-reference extraction.
fn tokenize(sql: &str) -> Vec<Token> {
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < len {
        match chars[i] {
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }

            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }

            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }

            // Double-quoted identifier: "name"
            '"' => {
                i += 1;
                let start = i;
                while i < len && chars[i] != '"' {
                    i += 1;
                }
                let ident = chars[start..i].iter().collect::<String>();
                if i < len {
                    i += 1; // closing quote
                }
                tokens.push(Token::Ident(ident));
            }

            // Backtick-quoted identifier: `name`
            '`' => {
                i += 1;
                let start = i;
                while i < len && chars[i] != '`' {
                    i += 1;
                }
                let ident = chars[start..i].iter().collect::<String>();
                if i < len {
                    i += 1;
                }
                tokens.push(Token::Ident(ident));
            }

            // Square-bracket quoted identifier: [name]
            '[' => {
                i += 1;
                let start = i;
                while i < len && chars[i] != ']' {
                    i += 1;
                }
                let ident = chars[start..i].iter().collect::<String>();
                if i < len {
                    i += 1;
                }
                tokens.push(Token::Ident(ident));
            }

            // Single-quoted string literal — not an identifier; skip it.
            '\'' => {
                i += 1;
                while i < len {
                    if chars[i] == '\'' {
                        // Handle escaped single quote ('').
                        if i + 1 < len && chars[i + 1] == '\'' {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                tokens.push(Token::Other);
            }

            c if c.is_alphanumeric() || c == '_' || c == '$' => {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
                {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                tokens.push(Token::Ident(word));
            }

            _ => {
                tokens.push(Token::Other);
                i += 1;
            }
        }
    }

    tokens
}

/// Parse the token stream and extract table references.
///
/// Recognizes the clauses: FROM, JOIN (any variant), UPDATE, INSERT INTO,
/// DELETE FROM. Skips tokens inside parentheses (subqueries).
fn parse_table_refs(tokens: &[Token]) -> Vec<QueryTableRef> {
    let mut refs = Vec::new();
    let mut depth: u32 = 0;
    let mut i = 0;

    while i < tokens.len() {
        match &tokens[i] {
            Token::LParen => {
                depth += 1;
                i += 1;
            }

            Token::RParen => {
                depth = depth.saturating_sub(1);
                i += 1;
            }

            Token::Other | Token::Dot => {
                i += 1;
            }

            Token::Ident(word) => {
                let upper = word.to_uppercase();

                let introduces_table = match upper.as_str() {
                    "FROM" | "JOIN" => true,
                    "UPDATE" => true,
                    "DELETE" => {
                        // DELETE FROM <table>: consume the FROM keyword next (skipping whitespace).
                        let mut j = i + 1;
                        while j < tokens.len() && tokens[j] == Token::Other {
                            j += 1;
                        }
                        if let Some(Token::Ident(next)) = tokens.get(j)
                            && next.to_uppercase() == "FROM"
                        {
                            i = j + 1;
                            if depth == 0
                                && let Some(table_ref) = read_qualified_name(tokens, &mut i)
                            {
                                refs.push(table_ref);
                            }
                            continue;
                        }
                        false
                    }
                    "INSERT" => {
                        // INSERT INTO <table>: consume INTO next (skipping whitespace).
                        let mut j = i + 1;
                        while j < tokens.len() && tokens[j] == Token::Other {
                            j += 1;
                        }
                        if let Some(Token::Ident(next)) = tokens.get(j)
                            && next.to_uppercase() == "INTO"
                        {
                            i = j + 1;
                            if depth == 0
                                && let Some(table_ref) = read_qualified_name(tokens, &mut i)
                            {
                                refs.push(table_ref);
                            }
                            continue;
                        }
                        false
                    }
                    _ => false,
                };

                if introduces_table {
                    i += 1;
                    if depth == 0
                        && let Some(table_ref) = read_qualified_name(tokens, &mut i)
                    {
                        refs.push(table_ref);
                    }
                } else {
                    i += 1;
                }
            }
        }
    }

    refs
}

/// Consume a (possibly qualified) table name starting at `*pos`.
///
/// Handles `table`, `schema.table`, and `db.schema.table` forms.
/// Advances `*pos` past the name (and an optional alias keyword).
/// Returns `None` if the next token is not an identifier.
fn read_qualified_name(tokens: &[Token], pos: &mut usize) -> Option<QueryTableRef> {
    // Skip any Other tokens (whitespace collapsed into Token::Other).
    while *pos < tokens.len() && tokens[*pos] == Token::Other {
        *pos += 1;
    }

    let first = match tokens.get(*pos) {
        Some(Token::Ident(s)) => {
            *pos += 1;
            s.clone()
        }
        _ => return None,
    };

    // Check for a dot, implying qualification.
    if tokens.get(*pos) == Some(&Token::Dot) {
        *pos += 1; // consume dot

        let second = match tokens.get(*pos) {
            Some(Token::Ident(s)) => {
                *pos += 1;
                s.clone()
            }
            _ => {
                // Trailing dot without an identifier; treat first as table name.
                return Some(QueryTableRef {
                    database: None,
                    schema: None,
                    table: first,
                });
            }
        };

        // Check for a second dot: db.schema.table
        if tokens.get(*pos) == Some(&Token::Dot) {
            *pos += 1;
            let third = match tokens.get(*pos) {
                Some(Token::Ident(s)) => {
                    *pos += 1;
                    s.clone()
                }
                _ => {
                    return Some(QueryTableRef {
                        database: None,
                        schema: Some(first),
                        table: second,
                    });
                }
            };
            skip_alias(tokens, pos);
            return Some(QueryTableRef {
                database: Some(first),
                schema: Some(second),
                table: third,
            });
        }

        skip_alias(tokens, pos);
        return Some(QueryTableRef {
            database: None,
            schema: Some(first),
            table: second,
        });
    }

    skip_alias(tokens, pos);
    Some(QueryTableRef {
        database: None,
        schema: None,
        table: first,
    })
}

/// Skip a potential alias token after a table name.
///
/// An alias is a bare identifier that is NOT a SQL keyword. This advances
/// `*pos` past it so subsequent keyword scans start at the right position.
fn skip_alias(tokens: &[Token], pos: &mut usize) {
    // Skip whitespace tokens.
    let start = *pos;
    while *pos < tokens.len() && tokens[*pos] == Token::Other {
        *pos += 1;
    }

    match tokens.get(*pos) {
        Some(Token::Ident(word)) => {
            let upper = word.to_uppercase();
            // If the next token is a reserved keyword, don't consume it.
            if is_reserved_keyword(&upper) {
                *pos = start;
            } else {
                *pos += 1;
            }
        }
        _ => {
            *pos = start;
        }
    }
}

fn is_reserved_keyword(word: &str) -> bool {
    matches!(
        word,
        "WHERE"
            | "SET"
            | "ON"
            | "AND"
            | "OR"
            | "NOT"
            | "AS"
            | "ORDER"
            | "GROUP"
            | "HAVING"
            | "LIMIT"
            | "OFFSET"
            | "UNION"
            | "EXCEPT"
            | "INTERSECT"
            | "LEFT"
            | "RIGHT"
            | "INNER"
            | "OUTER"
            | "CROSS"
            | "FULL"
            | "JOIN"
            | "FROM"
            | "INTO"
            | "SELECT"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "WITH"
            | "RETURNING"
            | "VALUES"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_names(query: &str) -> Vec<String> {
        extract_referenced_tables(query)
            .into_iter()
            .map(|r| r.table)
            .collect()
    }

    #[test]
    fn simple_select_from() {
        assert_eq!(table_names("SELECT * FROM users"), vec!["users"]);
    }

    #[test]
    fn schema_qualified_from() {
        let result = extract_referenced_tables("SELECT * FROM public.users");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].schema.as_deref(), Some("public"));
        assert_eq!(result[0].table, "users");
        assert_eq!(result[0].database, None);
    }

    #[test]
    fn from_with_join() {
        assert_eq!(
            table_names("SELECT * FROM users u JOIN orders o ON u.id = o.user_id"),
            vec!["users", "orders"]
        );
    }

    #[test]
    fn update_statement() {
        let result =
            extract_referenced_tables(r#"UPDATE "my schema"."t" SET col = 1 WHERE id = 2"#);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].schema.as_deref(), Some("my schema"));
        assert_eq!(result[0].table, "t");
    }

    #[test]
    fn insert_into() {
        assert_eq!(
            table_names("INSERT INTO users (a, b) VALUES (1, 2)"),
            vec!["users"]
        );
    }

    #[test]
    fn delete_from() {
        assert_eq!(table_names("DELETE FROM users WHERE id = 1"), vec!["users"]);
    }

    #[test]
    fn line_comment_stripped() {
        assert_eq!(
            table_names("-- FROM hidden\nSELECT * FROM real"),
            vec!["real"]
        );
    }

    #[test]
    fn block_comment_stripped() {
        assert_eq!(
            table_names("SELECT * /* FROM hidden */ FROM real"),
            vec!["real"]
        );
    }

    #[test]
    fn empty_query_returns_empty() {
        assert!(table_names("").is_empty());
    }

    #[test]
    fn invalid_query_returns_empty() {
        assert!(table_names("NOT A VALID QUERY AT ALL").is_empty());
    }

    #[test]
    fn left_join_variant() {
        assert_eq!(
            table_names("SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id"),
            vec!["orders", "users"]
        );
    }

    #[test]
    fn backtick_quoted_identifiers() {
        let result = extract_referenced_tables("SELECT * FROM `mydb`.`users`");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].schema.as_deref(), Some("mydb"));
        assert_eq!(result[0].table, "users");
    }

    #[test]
    fn three_part_name() {
        let result = extract_referenced_tables("SELECT * FROM mydb.public.users");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].database.as_deref(), Some("mydb"));
        assert_eq!(result[0].schema.as_deref(), Some("public"));
        assert_eq!(result[0].table, "users");
    }
}
