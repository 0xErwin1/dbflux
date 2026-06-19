// Types are consumed by the resolver (T12+) and public entry point (T19).
// Suppress dead_code until those modules are wired up.
#![allow(dead_code)]

use crate::query::visual_query::{BoolOp, Comparator, LiteralValue, PredicateValue};

/// Maximum number of path segments in a dotted LHS (`a.b.c.d.e` = 5).
pub const PATH_DEPTH_CAP: usize = 5;

// =============================================================================
// Token and Span
// =============================================================================

/// Byte-offset span in the source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// Unquoted identifier: `[A-Za-z_][A-Za-z0-9_]*`.
    Ident(String),
    /// `.` outside a string literal.
    Dot,
    /// Single- or double-quoted string literal (unescaped content).
    Str(String),
    /// Integer literal.
    Int(i64),
    /// Floating-point literal.
    Float(f64),
    /// Boolean keyword (`true` / `false`).
    Bool(bool),
    /// `NULL` keyword.
    Null,
    // --- Comparators ---
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Like,
    ILike,
    In,
    IsNull,
    IsNotNull,
    // --- Boolean operators ---
    And,
    Or,
    // --- Punctuation ---
    LParen,
    RParen,
    Comma,
    /// End of input.
    Eof,
}

#[derive(Debug, Clone)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Span,
}

// =============================================================================
// Tokenizer
// =============================================================================

struct Scanner<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src: src.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn peek2(&self) -> Option<u8> {
        self.src.get(self.pos + 1).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let ch = self.src.get(self.pos).copied();
        if ch.is_some() {
            self.pos += 1;
        }
        ch
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\r' | b'\n')) {
            self.advance();
        }
    }

    fn scan_identifier(&mut self, start: usize) -> SpannedToken {
        while matches!(
            self.peek(),
            Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
        ) {
            self.advance();
        }
        let word = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        let span = Span::new(start, self.pos);

        // Check for keyword comparators and boolean ops (case-insensitive).
        // Some keywords require a look-ahead at the rest of the input for
        // multi-word tokens like IS NULL / IS NOT NULL.
        let token = match word.to_uppercase().as_str() {
            "TRUE" => Token::Bool(true),
            "FALSE" => Token::Bool(false),
            "NULL" => Token::Null,
            "LIKE" => Token::Like,
            "ILIKE" => Token::ILike,
            "IN" => Token::In,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Ident("NOT".to_string()),
            "IS" => {
                // Peek ahead for IS NULL or IS NOT NULL.
                // Save position so we can restore on mismatch (shouldn't happen
                // in practice but is defensive).
                let saved = self.pos;
                self.skip_whitespace();
                if self.try_keyword_ci("NOT") {
                    self.skip_whitespace();
                    if self.try_keyword_ci("NULL") {
                        return SpannedToken {
                            token: Token::IsNotNull,
                            span: Span::new(start, self.pos),
                        };
                    }
                    // Restore — "IS NOT" without NULL is a parse error that
                    // will surface at the parser level.
                    self.pos = saved;
                } else if self.try_keyword_ci("NULL") {
                    return SpannedToken {
                        token: Token::IsNull,
                        span: Span::new(start, self.pos),
                    };
                } else {
                    self.pos = saved;
                }
                Token::Ident("IS".to_string())
            }
            _ => Token::Ident(word.to_string()),
        };

        SpannedToken { token, span }
    }

    /// Attempts to consume a case-insensitive keyword at the current position.
    /// Returns `true` and advances `pos` if the keyword matches; returns `false`
    /// and leaves `pos` unchanged otherwise.
    fn try_keyword_ci(&mut self, keyword: &str) -> bool {
        let kw = keyword.as_bytes();
        let end = self.pos + kw.len();
        if end > self.src.len() {
            return false;
        }
        let slice = &self.src[self.pos..end];
        let matches = slice
            .iter()
            .zip(kw.iter())
            .all(|(a, b)| a.eq_ignore_ascii_case(b));
        if matches {
            // Ensure the keyword is not a prefix of a longer identifier.
            let after = self.src.get(end).copied();
            let is_word_boundary =
                !matches!(after, Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'));
            if is_word_boundary {
                self.pos = end;
                return true;
            }
        }
        false
    }

    fn scan_string(&mut self, quote: u8, start: usize) -> Result<SpannedToken, ParseError> {
        let mut value = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(ParseError::UnterminatedString {
                        span: Span::new(start, self.pos),
                    });
                }
                Some(ch) if ch == quote => {
                    // Check for escaped quote ('' or "").
                    if self.peek() == Some(quote) {
                        self.advance();
                        value.push(quote as char);
                    } else {
                        break;
                    }
                }
                Some(ch) => {
                    value.push(ch as char);
                }
            }
        }
        Ok(SpannedToken {
            token: Token::Str(value),
            span: Span::new(start, self.pos),
        })
    }

    fn scan_number(&mut self, start: usize) -> Result<SpannedToken, ParseError> {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.advance();
        }

        let is_float = self.peek() == Some(b'.') && matches!(self.peek2(), Some(b'0'..=b'9'));

        if is_float {
            self.advance(); // consume the dot
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.advance();
            }
            let text = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
            match text.parse::<f64>() {
                Ok(f) => Ok(SpannedToken {
                    token: Token::Float(f),
                    span: Span::new(start, self.pos),
                }),
                Err(_) => Err(ParseError::InvalidNumber {
                    span: Span::new(start, self.pos),
                }),
            }
        } else {
            let text = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
            match text.parse::<i64>() {
                Ok(i) => Ok(SpannedToken {
                    token: Token::Int(i),
                    span: Span::new(start, self.pos),
                }),
                Err(_) => Err(ParseError::InvalidNumber {
                    span: Span::new(start, self.pos),
                }),
            }
        }
    }

    fn next_token(&mut self) -> Result<SpannedToken, ParseError> {
        self.skip_whitespace();
        let start = self.pos;

        match self.advance() {
            None => Ok(SpannedToken {
                token: Token::Eof,
                span: Span::new(start, start),
            }),
            Some(b'.') => Ok(SpannedToken {
                token: Token::Dot,
                span: Span::new(start, self.pos),
            }),
            Some(b'(') => Ok(SpannedToken {
                token: Token::LParen,
                span: Span::new(start, self.pos),
            }),
            Some(b')') => Ok(SpannedToken {
                token: Token::RParen,
                span: Span::new(start, self.pos),
            }),
            Some(b',') => Ok(SpannedToken {
                token: Token::Comma,
                span: Span::new(start, self.pos),
            }),
            Some(b'=') => Ok(SpannedToken {
                token: Token::Eq,
                span: Span::new(start, self.pos),
            }),
            Some(b'<') => {
                if self.peek() == Some(b'>') {
                    self.advance();
                    Ok(SpannedToken {
                        token: Token::Neq,
                        span: Span::new(start, self.pos),
                    })
                } else if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(SpannedToken {
                        token: Token::Lte,
                        span: Span::new(start, self.pos),
                    })
                } else {
                    Ok(SpannedToken {
                        token: Token::Lt,
                        span: Span::new(start, self.pos),
                    })
                }
            }
            Some(b'>') => {
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(SpannedToken {
                        token: Token::Gte,
                        span: Span::new(start, self.pos),
                    })
                } else {
                    Ok(SpannedToken {
                        token: Token::Gt,
                        span: Span::new(start, self.pos),
                    })
                }
            }
            Some(b'!') => {
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(SpannedToken {
                        token: Token::Neq,
                        span: Span::new(start, self.pos),
                    })
                } else {
                    Err(ParseError::UnexpectedChar {
                        ch: '!',
                        span: Span::new(start, self.pos),
                    })
                }
            }
            Some(q @ (b'\'' | b'"')) => self.scan_string(q, start),
            Some(b'-') => {
                // Could be a negative number.
                if matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.scan_number(start)
                } else {
                    Err(ParseError::UnexpectedChar {
                        ch: '-',
                        span: Span::new(start, self.pos),
                    })
                }
            }
            Some(b'0'..=b'9') => self.scan_number(start),
            Some(ch) if (ch as char).is_alphabetic() || ch == b'_' => {
                Ok(self.scan_identifier(start))
            }
            Some(ch) => Err(ParseError::UnexpectedChar {
                ch: ch as char,
                span: Span::new(start, self.pos),
            }),
        }
    }
}

/// Tokenize the full input string into a `Vec<SpannedToken>`.
///
/// Exposed for testing; normal callers use `parse()` directly.
pub fn tokenize(input: &str) -> Result<Vec<SpannedToken>, ParseError> {
    let mut scanner = Scanner::new(input);
    let mut tokens = Vec::new();
    loop {
        let tok = scanner.next_token()?;
        let is_eof = tok.token == Token::Eof;
        tokens.push(tok);
        if is_eof {
            break;
        }
    }
    Ok(tokens)
}

// =============================================================================
// AST
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum Lhs {
    BareColumn(String),
    DottedPath { segments: Vec<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedPredicate {
    pub lhs: Lhs,
    pub span: Span,
    pub comparator: Comparator,
    pub rhs: PredicateValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    Predicate(ParsedPredicate),
    Bool {
        op: BoolOp,
        children: Vec<FilterExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationalFilterAst {
    pub root: FilterExpr,
}

// =============================================================================
// Errors
// =============================================================================

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ParseError {
    #[error("unexpected character '{ch}' at {span:?}")]
    UnexpectedChar { ch: char, span: Span },

    #[error("unexpected token at {span:?}")]
    UnexpectedToken { span: Span },

    #[error("unterminated string starting at {span:?}")]
    UnterminatedString { span: Span },

    #[error("invalid number at {span:?}")]
    InvalidNumber { span: Span },

    #[error("invalid IN list at {span:?}")]
    InvalidInList { span: Span },

    #[error("empty input")]
    EmptyInput,

    #[error("path too deep: found {found} segments, cap is {cap}")]
    PathTooDeep { found: usize, cap: usize },
}

// =============================================================================
// Parser
// =============================================================================

struct Parser {
    tokens: Vec<SpannedToken>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<SpannedToken>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.pos)
            .map(|t| &t.token)
            .unwrap_or(&Token::Eof)
    }

    fn peek_span(&self) -> Span {
        self.tokens
            .get(self.pos)
            .map(|t| t.span)
            .unwrap_or(Span::new(0, 0))
    }

    fn advance(&mut self) -> &SpannedToken {
        let tok = &self.tokens[self.pos];
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn expect_ident(&mut self) -> Result<(String, Span), ParseError> {
        let span = self.peek_span();
        if let Token::Ident(name) = self.peek().clone() {
            self.advance();
            Ok((name, span))
        } else {
            Err(ParseError::UnexpectedToken { span })
        }
    }

    /// Parse the full expression (entry point).
    fn parse_expr(&mut self) -> Result<FilterExpr, ParseError> {
        if matches!(self.peek(), Token::Eof) {
            return Err(ParseError::EmptyInput);
        }
        let expr = self.parse_or()?;

        if !matches!(self.peek(), Token::Eof) {
            return Err(ParseError::UnexpectedToken {
                span: self.peek_span(),
            });
        }
        Ok(expr)
    }

    fn parse_or(&mut self) -> Result<FilterExpr, ParseError> {
        let first = self.parse_and()?;
        if !matches!(self.peek(), Token::Or) {
            return Ok(first);
        }

        let mut children = vec![first];
        while matches!(self.peek(), Token::Or) {
            self.advance();
            children.push(self.parse_and()?);
        }
        Ok(FilterExpr::Bool {
            op: BoolOp::Or,
            children,
        })
    }

    fn parse_and(&mut self) -> Result<FilterExpr, ParseError> {
        let first = self.parse_atom()?;
        if !matches!(self.peek(), Token::And) {
            return Ok(first);
        }

        let mut children = vec![first];
        while matches!(self.peek(), Token::And) {
            self.advance();
            children.push(self.parse_atom()?);
        }
        Ok(FilterExpr::Bool {
            op: BoolOp::And,
            children,
        })
    }

    fn parse_atom(&mut self) -> Result<FilterExpr, ParseError> {
        if matches!(self.peek(), Token::LParen) {
            self.advance();
            let inner = self.parse_or()?;
            if !matches!(self.peek(), Token::RParen) {
                return Err(ParseError::UnexpectedToken {
                    span: self.peek_span(),
                });
            }
            self.advance();
            return Ok(inner);
        }
        self.parse_predicate().map(FilterExpr::Predicate)
    }

    fn parse_predicate(&mut self) -> Result<ParsedPredicate, ParseError> {
        let start_span = self.peek_span();
        let lhs = self.parse_lhs()?;
        let comparator = self.parse_comparator()?;

        let rhs = match comparator {
            Comparator::IsNull | Comparator::IsNotNull => PredicateValue::None,
            Comparator::In => self.parse_in_list()?,
            _ => {
                let val = self.parse_single_value()?;
                PredicateValue::Single(val)
            }
        };

        Ok(ParsedPredicate {
            lhs,
            span: Span::new(start_span.start, self.peek_span().start),
            comparator,
            rhs,
        })
    }

    fn parse_lhs(&mut self) -> Result<Lhs, ParseError> {
        let (first, _) = self.expect_ident()?;
        let mut segments = vec![first];

        while matches!(self.peek(), Token::Dot) {
            self.advance(); // consume dot
            let (seg, _) = self.expect_ident()?;
            segments.push(seg);

            if segments.len() > PATH_DEPTH_CAP {
                return Err(ParseError::PathTooDeep {
                    found: segments.len(),
                    cap: PATH_DEPTH_CAP,
                });
            }
        }

        if segments.len() == 1 {
            Ok(Lhs::BareColumn(segments.remove(0)))
        } else {
            Ok(Lhs::DottedPath { segments })
        }
    }

    fn parse_comparator(&mut self) -> Result<Comparator, ParseError> {
        let span = self.peek_span();
        let tok = self.peek().clone();
        match tok {
            Token::Eq => {
                self.advance();
                Ok(Comparator::Eq)
            }
            Token::Neq => {
                self.advance();
                Ok(Comparator::Neq)
            }
            Token::Lt => {
                self.advance();
                Ok(Comparator::Lt)
            }
            Token::Lte => {
                self.advance();
                Ok(Comparator::Lte)
            }
            Token::Gt => {
                self.advance();
                Ok(Comparator::Gt)
            }
            Token::Gte => {
                self.advance();
                Ok(Comparator::Gte)
            }
            Token::Like => {
                self.advance();
                Ok(Comparator::Like)
            }
            Token::ILike => {
                self.advance();
                Ok(Comparator::ILike)
            }
            Token::In => {
                self.advance();
                Ok(Comparator::In)
            }
            Token::IsNull => {
                self.advance();
                Ok(Comparator::IsNull)
            }
            Token::IsNotNull => {
                self.advance();
                Ok(Comparator::IsNotNull)
            }
            _ => Err(ParseError::UnexpectedToken { span }),
        }
    }

    fn parse_single_value(&mut self) -> Result<LiteralValue, ParseError> {
        let span = self.peek_span();
        let tok = self.peek().clone();
        match tok {
            Token::Str(s) => {
                self.advance();
                Ok(LiteralValue::Text(s))
            }
            Token::Int(i) => {
                self.advance();
                Ok(LiteralValue::Integer(i))
            }
            Token::Float(f) => {
                self.advance();
                Ok(LiteralValue::Float(f))
            }
            Token::Bool(b) => {
                self.advance();
                Ok(LiteralValue::Bool(b))
            }
            Token::Null => {
                self.advance();
                Ok(LiteralValue::Null)
            }
            _ => Err(ParseError::UnexpectedToken { span }),
        }
    }

    fn parse_in_list(&mut self) -> Result<PredicateValue, ParseError> {
        let span = self.peek_span();
        if !matches!(self.peek(), Token::LParen) {
            return Err(ParseError::InvalidInList { span });
        }
        self.advance();

        if matches!(self.peek(), Token::RParen) {
            return Err(ParseError::InvalidInList {
                span: self.peek_span(),
            });
        }

        let mut items = Vec::new();
        loop {
            let val = self.parse_single_value()?;
            items.push(LiteralValue::into_predicate_single(val));

            match self.peek() {
                Token::Comma => {
                    self.advance();
                }
                Token::RParen => {
                    self.advance();
                    break;
                }
                _ => {
                    return Err(ParseError::InvalidInList {
                        span: self.peek_span(),
                    });
                }
            }
        }

        Ok(PredicateValue::List(items))
    }
}

trait IntoPredSingle {
    fn into_predicate_single(self) -> LiteralValue;
}

impl IntoPredSingle for LiteralValue {
    fn into_predicate_single(self) -> LiteralValue {
        self
    }
}

// =============================================================================
// Public entry
// =============================================================================

/// Parse the filter text into an AST.
pub fn parse(input: &str) -> Result<RelationalFilterAst, ParseError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(tokens);
    let root = parser.parse_expr()?;
    Ok(RelationalFilterAst { root })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(input: &str) -> Vec<Token> {
        tokenize(input)
            .unwrap()
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    // T04: identifiers, dots, whitespace, EOF
    #[test]
    fn tokenize_identifier_and_dot() {
        assert_eq!(
            tok("user.email"),
            vec![
                Token::Ident("user".into()),
                Token::Dot,
                Token::Ident("email".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn tokenize_whitespace_skipped() {
        assert_eq!(
            tok("  a  .  b  "),
            vec![
                Token::Ident("a".into()),
                Token::Dot,
                Token::Ident("b".into()),
                Token::Eof,
            ]
        );
    }

    // T05: string literals with escape and dots inside
    #[test]
    fn tokenize_string_literal_with_dot() {
        let tokens = tok("email = 'a.b@x.com'");
        // Must NOT contain a Dot token — the dot is inside the literal.
        assert!(!tokens.contains(&Token::Dot));
        assert!(tokens.contains(&Token::Str("a.b@x.com".into())));
    }

    #[test]
    fn tokenize_single_quote_escape() {
        assert_eq!(
            tok("'it''s fine'"),
            vec![Token::Str("it's fine".into()), Token::Eof]
        );
    }

    #[test]
    fn tokenize_double_quote_string() {
        assert_eq!(tok("\"a.b\""), vec![Token::Str("a.b".into()), Token::Eof]);
    }

    #[test]
    fn tokenize_double_quote_escape() {
        assert_eq!(
            tok("\"he said \"\"hi\"\"\""),
            vec![Token::Str("he said \"hi\"".into()), Token::Eof]
        );
    }

    #[test]
    fn tokenize_unterminated_string() {
        assert!(matches!(
            tokenize("'unclosed"),
            Err(ParseError::UnterminatedString { .. })
        ));
    }

    // T06: integers, floats, booleans, NULL
    #[test]
    #[allow(clippy::approx_constant)]
    fn tokenize_number_and_bool() {
        assert_eq!(
            tok("42 3.14 true FALSE null"),
            vec![
                Token::Int(42),
                Token::Float(3.14),
                Token::Bool(true),
                Token::Bool(false),
                Token::Null,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn tokenize_float_no_spurious_dot() {
        let tokens = tok("1.5");
        assert!(!tokens.contains(&Token::Dot));
        assert!(matches!(tokens[0], Token::Float(f) if (f - 1.5).abs() < 1e-10));
    }

    #[test]
    fn tokenize_negative_integer() {
        assert_eq!(tok("-7"), vec![Token::Int(-7), Token::Eof]);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn tokenize_negative_float() {
        let tokens = tok("-3.14");
        assert!(matches!(tokens[0], Token::Float(f) if (f - (-3.14)).abs() < 1e-10));
    }

    // T07: comparators
    #[test]
    fn tokenize_all_comparators() {
        assert_eq!(tok("="), vec![Token::Eq, Token::Eof]);
        assert_eq!(tok("<>"), vec![Token::Neq, Token::Eof]);
        assert_eq!(tok("!="), vec![Token::Neq, Token::Eof]);
        assert_eq!(tok("<"), vec![Token::Lt, Token::Eof]);
        assert_eq!(tok("<="), vec![Token::Lte, Token::Eof]);
        assert_eq!(tok(">"), vec![Token::Gt, Token::Eof]);
        assert_eq!(tok(">="), vec![Token::Gte, Token::Eof]);
        assert_eq!(tok("LIKE"), vec![Token::Like, Token::Eof]);
        assert_eq!(tok("ILIKE"), vec![Token::ILike, Token::Eof]);
        assert_eq!(tok("IN"), vec![Token::In, Token::Eof]);
        assert_eq!(tok("IS NULL"), vec![Token::IsNull, Token::Eof]);
        assert_eq!(tok("IS NOT NULL"), vec![Token::IsNotNull, Token::Eof]);
    }

    #[test]
    fn tokenize_bool_ops_and_parens() {
        assert_eq!(tok("AND"), vec![Token::And, Token::Eof]);
        assert_eq!(tok("OR"), vec![Token::Or, Token::Eof]);
        assert_eq!(tok("("), vec![Token::LParen, Token::Eof]);
        assert_eq!(tok(")"), vec![Token::RParen, Token::Eof]);
        assert_eq!(tok(","), vec![Token::Comma, Token::Eof]);
    }

    // T08: LHS recognition and depth cap
    #[test]
    fn parse_bare_column_lhs() {
        let ast = parse("status = 1").unwrap();
        if let FilterExpr::Predicate(pred) = ast.root {
            assert_eq!(pred.lhs, Lhs::BareColumn("status".into()));
        } else {
            panic!("expected Predicate");
        }
    }

    #[test]
    fn parse_dotted_path_lhs() {
        let ast = parse("user.email = 'x'").unwrap();
        if let FilterExpr::Predicate(pred) = ast.root {
            assert_eq!(
                pred.lhs,
                Lhs::DottedPath {
                    segments: vec!["user".into(), "email".into()]
                }
            );
        } else {
            panic!("expected Predicate");
        }
    }

    #[test]
    fn parse_depth_cap_exceeded() {
        // 6 segments: a.b.c.d.e.f
        assert!(matches!(
            parse("a.b.c.d.e.f = 1"),
            Err(ParseError::PathTooDeep { found: 6, cap: 5 })
        ));
    }

    #[test]
    fn parse_max_depth_exactly_5_ok() {
        // 5 segments: a.b.c.d.e
        assert!(parse("a.b.c.d.e = 1").is_ok());
    }

    // T09: value RHS
    #[test]
    #[allow(clippy::approx_constant)]
    fn parse_rhs_values() {
        let ast_str = parse("col = 'hello'").unwrap();
        let ast_int = parse("col = 42").unwrap();
        let ast_float = parse("col = 3.14").unwrap();
        let ast_bool = parse("col = true").unwrap();
        let ast_null = parse("col = null").unwrap();

        let pred_val = |ast: RelationalFilterAst| {
            if let FilterExpr::Predicate(p) = ast.root {
                p.rhs
            } else {
                panic!("expected predicate")
            }
        };

        assert_eq!(
            pred_val(ast_str),
            PredicateValue::Single(LiteralValue::Text("hello".into()))
        );
        assert_eq!(
            pred_val(ast_int),
            PredicateValue::Single(LiteralValue::Integer(42))
        );
        assert!(
            matches!(pred_val(ast_float), PredicateValue::Single(LiteralValue::Float(f)) if (f - 3.14).abs() < 1e-10)
        );
        assert_eq!(
            pred_val(ast_bool),
            PredicateValue::Single(LiteralValue::Bool(true))
        );
        assert_eq!(
            pred_val(ast_null),
            PredicateValue::Single(LiteralValue::Null)
        );
    }

    #[test]
    fn parse_in_list() {
        let ast = parse("id IN (1, 2, 3)").unwrap();
        if let FilterExpr::Predicate(pred) = ast.root {
            assert_eq!(pred.comparator, Comparator::In);
            assert_eq!(
                pred.rhs,
                PredicateValue::List(vec![
                    LiteralValue::Integer(1),
                    LiteralValue::Integer(2),
                    LiteralValue::Integer(3),
                ])
            );
        } else {
            panic!("expected predicate");
        }
    }

    #[test]
    fn parse_empty_in_list_error() {
        assert!(matches!(
            parse("id IN ()"),
            Err(ParseError::InvalidInList { .. })
        ));
    }

    // T10: full single predicate
    #[test]
    fn parse_single_predicate_eq() {
        let ast = parse("status = 'active'").unwrap();
        if let FilterExpr::Predicate(pred) = ast.root {
            assert_eq!(pred.lhs, Lhs::BareColumn("status".into()));
            assert_eq!(pred.comparator, Comparator::Eq);
            assert_eq!(
                pred.rhs,
                PredicateValue::Single(LiteralValue::Text("active".into()))
            );
        } else {
            panic!("expected Predicate");
        }
    }

    #[test]
    fn parse_is_null_no_rhs() {
        let ast = parse("col IS NULL").unwrap();
        if let FilterExpr::Predicate(pred) = ast.root {
            assert_eq!(pred.comparator, Comparator::IsNull);
            assert_eq!(pred.rhs, PredicateValue::None);
        } else {
            panic!("expected Predicate");
        }
    }

    #[test]
    fn parse_is_not_null_no_rhs() {
        let ast = parse("col IS NOT NULL").unwrap();
        if let FilterExpr::Predicate(pred) = ast.root {
            assert_eq!(pred.comparator, Comparator::IsNotNull);
            assert_eq!(pred.rhs, PredicateValue::None);
        } else {
            panic!("expected Predicate");
        }
    }

    // T11: AND/OR composition and parenthesised groups
    #[test]
    fn parse_and_composition() {
        let ast = parse("a = 1 AND b = 2 AND c = 3").unwrap();
        if let FilterExpr::Bool { op, children } = ast.root {
            assert_eq!(op, BoolOp::And);
            assert_eq!(children.len(), 3);
        } else {
            panic!("expected Bool");
        }
    }

    #[test]
    fn parse_and_binds_tighter_than_or() {
        // a = 1 OR b = 2 AND c = 3  =>  OR( a=1, AND(b=2, c=3) )
        let ast = parse("a = 1 OR b = 2 AND c = 3").unwrap();
        if let FilterExpr::Bool { op, children } = ast.root {
            assert_eq!(op, BoolOp::Or);
            assert_eq!(children.len(), 2);
            if let FilterExpr::Bool {
                op: inner_op,
                children: inner_children,
            } = &children[1]
            {
                assert_eq!(*inner_op, BoolOp::And);
                assert_eq!(inner_children.len(), 2);
            } else {
                panic!("second child of OR must be AND");
            }
        } else {
            panic!("expected Or at root");
        }
    }

    #[test]
    fn parse_parens_override_precedence() {
        // (a = 1 OR b = 2) AND c = 3  =>  AND( OR(a=1,b=2), c=3 )
        let ast = parse("(a = 1 OR b = 2) AND c = 3").unwrap();
        if let FilterExpr::Bool { op, children } = ast.root {
            assert_eq!(op, BoolOp::And);
            assert_eq!(children.len(), 2);
            if let FilterExpr::Bool {
                op: inner_op,
                children: inner_children,
            } = &children[0]
            {
                assert_eq!(*inner_op, BoolOp::Or);
                assert_eq!(inner_children.len(), 2);
            } else {
                panic!("first child of AND must be OR");
            }
        } else {
            panic!("expected And at root");
        }
    }

    #[test]
    fn parse_trailing_token_error() {
        assert!(matches!(
            parse("a = 1 extra"),
            Err(ParseError::UnexpectedToken { .. })
        ));
    }

    #[test]
    fn parse_empty_input_error() {
        assert!(matches!(parse(""), Err(ParseError::EmptyInput)));
    }

    // S-04: dots inside string literals are NOT split
    #[test]
    fn parse_dot_in_string_literal_is_bare_column() {
        let ast = parse("email = 'a.b@x.com'").unwrap();
        if let FilterExpr::Predicate(pred) = ast.root {
            assert_eq!(pred.lhs, Lhs::BareColumn("email".into()));
            assert_eq!(
                pred.rhs,
                PredicateValue::Single(LiteralValue::Text("a.b@x.com".into()))
            );
        } else {
            panic!("expected Predicate");
        }
    }
}
