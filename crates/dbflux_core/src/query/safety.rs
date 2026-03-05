#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanState {
    Normal,
    LineComment,
    BlockComment,
    SingleQuote,
    DoubleQuote,
}

pub fn is_safe_read_query(sql: &str) -> bool {
    let stripped = strip_comments(sql);
    let trimmed = stripped.trim();

    if trimmed.is_empty() {
        return false;
    }

    if has_multiple_statements(trimmed) {
        return false;
    }

    let Some(keyword) = first_keyword(trimmed) else {
        return false;
    };

    matches!(
        keyword.as_str(),
        "SELECT" | "SHOW" | "EXPLAIN" | "WITH" | "DESC" | "DESCRIBE"
    )
}

fn strip_comments(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut result = String::with_capacity(sql.len());
    let mut index = 0;
    let mut state = ScanState::Normal;

    while index < chars.len() {
        let current = chars[index];
        let next = chars.get(index + 1).copied();

        match state {
            ScanState::Normal => {
                if current == '-' && next == Some('-') {
                    state = ScanState::LineComment;
                    index += 2;
                    continue;
                }

                if current == '/' && next == Some('*') {
                    state = ScanState::BlockComment;
                    index += 2;
                    continue;
                }

                if current == '\'' {
                    state = ScanState::SingleQuote;
                } else if current == '"' {
                    state = ScanState::DoubleQuote;
                }

                result.push(current);
                index += 1;
            }

            ScanState::LineComment => {
                if current == '\n' {
                    result.push('\n');
                    state = ScanState::Normal;
                }
                index += 1;
            }

            ScanState::BlockComment => {
                if current == '*' && next == Some('/') {
                    state = ScanState::Normal;
                    index += 2;
                } else {
                    index += 1;
                }
            }

            ScanState::SingleQuote => {
                result.push(current);

                if current == '\'' {
                    if next == Some('\'') {
                        result.push('\'');
                        index += 2;
                        continue;
                    }
                    state = ScanState::Normal;
                }

                index += 1;
            }

            ScanState::DoubleQuote => {
                result.push(current);

                if current == '"' {
                    if next == Some('"') {
                        result.push('"');
                        index += 2;
                        continue;
                    }
                    state = ScanState::Normal;
                }

                index += 1;
            }
        }
    }

    result
}

fn has_multiple_statements(sql: &str) -> bool {
    let mut state = ScanState::Normal;
    let mut seen_semicolon = false;
    let chars: Vec<char> = sql.chars().collect();
    let mut index = 0;

    while index < chars.len() {
        let current = chars[index];
        let next = chars.get(index + 1).copied();

        match state {
            ScanState::Normal => {
                if current == '\'' {
                    state = ScanState::SingleQuote;
                } else if current == '"' {
                    state = ScanState::DoubleQuote;
                } else if current == ';' {
                    seen_semicolon = true;
                } else if seen_semicolon && !current.is_whitespace() {
                    return true;
                }
            }

            ScanState::SingleQuote => {
                if current == '\'' {
                    if next == Some('\'') {
                        index += 1;
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }

            ScanState::DoubleQuote => {
                if current == '"' {
                    if next == Some('"') {
                        index += 1;
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }

            ScanState::LineComment | ScanState::BlockComment => {}
        }

        index += 1;
    }

    false
}

fn first_keyword(sql: &str) -> Option<String> {
    sql.split_whitespace()
        .map(|part| part.trim_start_matches(|c: char| !c.is_ascii_alphabetic()))
        .find(|part| !part.is_empty())
        .map(|part| {
            part.chars()
                .take_while(|ch| ch.is_ascii_alphabetic())
                .collect::<String>()
                .to_ascii_uppercase()
        })
        .filter(|word| !word.is_empty())
}

#[cfg(test)]
mod tests {
    use super::is_safe_read_query;

    #[test]
    fn allows_basic_read_queries() {
        assert!(is_safe_read_query("SELECT * FROM users"));
        assert!(is_safe_read_query(
            "with cte as (select 1) select * from cte"
        ));
        assert!(is_safe_read_query("SHOW TABLES"));
        assert!(is_safe_read_query("DESC users"));
    }

    #[test]
    fn rejects_write_queries() {
        assert!(!is_safe_read_query("INSERT INTO users VALUES (1)"));
        assert!(!is_safe_read_query("UPDATE users SET name = 'a'"));
        assert!(!is_safe_read_query("DELETE FROM users"));
        assert!(!is_safe_read_query("DROP TABLE users"));
    }

    #[test]
    fn rejects_multiple_statements() {
        assert!(!is_safe_read_query("SELECT 1; DROP TABLE users"));
        assert!(!is_safe_read_query("SELECT 1; SELECT 2"));
    }

    #[test]
    fn allows_single_statement_with_trailing_semicolon() {
        assert!(is_safe_read_query("SELECT 1;"));
        assert!(is_safe_read_query("-- comment\nSELECT 1;"));
    }

    #[test]
    fn strips_comments_before_keyword_detection() {
        assert!(is_safe_read_query("-- hello\nSELECT * FROM users"));
        assert!(is_safe_read_query("/* hello */ SELECT * FROM users"));
        assert!(!is_safe_read_query("/* hello */ DELETE FROM users"));
    }
}
