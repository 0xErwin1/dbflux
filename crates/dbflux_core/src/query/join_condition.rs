//! Strict parser for join conditions supplied as text by a non-interactive
//! caller.
//!
//! The visual query builder emits both sides of a [`JoinPredicate`] verbatim,
//! so text from an untrusted caller must never reach it unparsed. This parser
//! accepts only column-to-column comparisons joined by `AND` and returns them
//! as structured references, which the caller renders through its dialect.

use crate::query::visual_query::{BoolOp, Comparator, JoinFilterNode, JoinOn, JoinPredicate};

/// The accepted form, shown in every rejection.
pub const JOIN_CONDITION_FORM: &str = "Accepted form: 'a.col = b.col' comparisons joined by AND, \
     with operators = != <> < <= > >=. Each side is qualifier.column, where the qualifier is \
     the name or alias of the main table, of this join or of an earlier join.";

/// One side of a join comparison: a column of a table in the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinColumnRef {
    pub qualifier: String,
    pub column: String,
}

/// A parsed `left <op> right` comparison between two columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinComparison {
    pub left: JoinColumnRef,
    pub op: Comparator,
    pub right: JoinColumnRef,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JoinConditionError {
    #[error("join condition is empty. {}", JOIN_CONDITION_FORM)]
    Empty,

    #[error(
        "join condition contains '{character}', which is not accepted. {}",
        JOIN_CONDITION_FORM
    )]
    UnexpectedCharacter { character: char },

    #[error("join condition is malformed: {reason}. {}", JOIN_CONDITION_FORM)]
    Malformed { reason: String },

    #[error(
        "join condition references '{qualifier}', which is not a table in this query (known: {}). {}",
        .known.join(", "),
        JOIN_CONDITION_FORM
    )]
    UnknownQualifier {
        qualifier: String,
        known: Vec<String>,
    },
}

/// Whether `text` is a plain identifier: ASCII letters, digits and
/// underscores, not starting with a digit.
pub fn is_plain_sql_identifier(text: &str) -> bool {
    let mut characters = text.chars();

    let starts_well = characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_');

    starts_well && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Identifier(String),
    Dot,
    Operator(Comparator),
}

/// Parses `on` into column comparisons. Every qualifier must be one of
/// `known_qualifiers`; anything outside the accepted form is rejected.
pub fn parse_join_condition(
    on: &str,
    known_qualifiers: &[&str],
) -> Result<Vec<JoinComparison>, JoinConditionError> {
    let tokens = tokenize(on)?;

    if tokens.is_empty() {
        return Err(JoinConditionError::Empty);
    }

    let mut position = 0;
    let mut comparisons = Vec::new();

    loop {
        let left = parse_column_ref(&tokens, &mut position, known_qualifiers)?;

        let op = match tokens.get(position) {
            Some(Token::Operator(op)) => *op,
            _ => {
                return Err(malformed(format!(
                    "expected a comparison operator after {}.{}",
                    left.qualifier, left.column
                )));
            }
        };
        position += 1;

        let right = parse_column_ref(&tokens, &mut position, known_qualifiers)?;
        comparisons.push(JoinComparison { left, op, right });

        match tokens.get(position) {
            None => return Ok(comparisons),
            Some(Token::Identifier(word)) if word.eq_ignore_ascii_case("and") => position += 1,
            Some(Token::Identifier(word)) => {
                return Err(malformed(format!(
                    "comparisons can only be joined by AND, found '{word}'"
                )));
            }
            Some(_) => {
                return Err(malformed(
                    "expected AND or the end of the condition after a comparison",
                ));
            }
        }
    }
}

/// Builds the structured `ON` clause for `comparisons`. `render` turns each
/// column reference into the SQL the generator emits verbatim, so it must
/// quote both parts through the target dialect.
pub fn join_on_conditions(
    comparisons: &[JoinComparison],
    render: impl Fn(&JoinColumnRef) -> String,
) -> JoinOn {
    let children = comparisons
        .iter()
        .map(|comparison| {
            JoinFilterNode::Predicate(JoinPredicate {
                node_id: 0,
                left: render(&comparison.left),
                op: comparison.op,
                right: render(&comparison.right),
            })
        })
        .collect();

    JoinOn::Conditions(JoinFilterNode::Group {
        node_id: 0,
        op: BoolOp::And,
        children,
    })
}

fn malformed(reason: impl Into<String>) -> JoinConditionError {
    JoinConditionError::Malformed {
        reason: reason.into(),
    }
}

fn tokenize(on: &str) -> Result<Vec<Token>, JoinConditionError> {
    let characters: Vec<char> = on.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;

    while let Some(&current) = characters.get(index) {
        let next = characters.get(index + 1).copied();

        if current.is_whitespace() {
            index += 1;
            continue;
        }

        if current.is_ascii_alphabetic() || current == '_' {
            let start = index;

            while characters
                .get(index)
                .is_some_and(|character| character.is_ascii_alphanumeric() || *character == '_')
            {
                index += 1;
            }

            tokens.push(Token::Identifier(characters[start..index].iter().collect()));
            continue;
        }

        let (token, length) = match (current, next) {
            ('.', _) => (Token::Dot, 1),
            ('=', _) => (Token::Operator(Comparator::Eq), 1),
            ('!', Some('=')) | ('<', Some('>')) => (Token::Operator(Comparator::Neq), 2),
            ('<', Some('=')) => (Token::Operator(Comparator::Lte), 2),
            ('>', Some('=')) => (Token::Operator(Comparator::Gte), 2),
            ('<', _) => (Token::Operator(Comparator::Lt), 1),
            ('>', _) => (Token::Operator(Comparator::Gt), 1),
            _ => return Err(JoinConditionError::UnexpectedCharacter { character: current }),
        };

        tokens.push(token);
        index += length;
    }

    Ok(tokens)
}

fn parse_column_ref(
    tokens: &[Token],
    position: &mut usize,
    known_qualifiers: &[&str],
) -> Result<JoinColumnRef, JoinConditionError> {
    let (qualifier, column) = match (
        tokens.get(*position),
        tokens.get(*position + 1),
        tokens.get(*position + 2),
    ) {
        (Some(Token::Identifier(qualifier)), Some(Token::Dot), Some(Token::Identifier(column))) => {
            (qualifier, column)
        }
        _ => {
            return Err(malformed("expected a column written as qualifier.column"));
        }
    };

    *position += 3;

    if !known_qualifiers.contains(&qualifier.as_str()) {
        return Err(JoinConditionError::UnknownQualifier {
            qualifier: qualifier.clone(),
            known: known_qualifiers
                .iter()
                .map(|name| name.to_string())
                .collect(),
        });
    }

    Ok(JoinColumnRef {
        qualifier: qualifier.clone(),
        column: column.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KNOWN: &[&str] = &["a", "b", "users", "orders"];

    fn column(qualifier: &str, column: &str) -> JoinColumnRef {
        JoinColumnRef {
            qualifier: qualifier.to_string(),
            column: column.to_string(),
        }
    }

    #[test]
    fn parses_a_single_equality() {
        let parsed = parse_join_condition("users.id = orders.user_id", KNOWN);

        assert_eq!(
            parsed,
            Ok(vec![JoinComparison {
                left: column("users", "id"),
                op: Comparator::Eq,
                right: column("orders", "user_id"),
            }])
        );
    }

    #[test]
    fn parses_comparisons_joined_by_and_in_any_case() {
        let parsed = parse_join_condition("a.id=b.a_id AnD a.tenant = b.tenant and a.x<b.y", KNOWN)
            .expect("the condition is in the accepted form");

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[1].left, column("a", "tenant"));
        assert_eq!(parsed[2].op, Comparator::Lt);
    }

    #[test]
    fn parses_every_supported_operator() {
        let cases = [
            ("=", Comparator::Eq),
            ("!=", Comparator::Neq),
            ("<>", Comparator::Neq),
            ("<", Comparator::Lt),
            ("<=", Comparator::Lte),
            (">", Comparator::Gt),
            (">=", Comparator::Gte),
        ];

        for (symbol, expected) in cases {
            let parsed = parse_join_condition(&format!("a.x {symbol} b.y"), KNOWN)
                .unwrap_or_else(|error| panic!("'{symbol}' should parse: {error}"));

            assert_eq!(parsed[0].op, expected, "operator {symbol}");
        }
    }

    #[test]
    fn rejects_everything_outside_the_accepted_form() {
        let hostile = [
            "",
            "   ",
            "a.id = b.id; DROP TABLE x",
            "a.id = b.id OR 1=1",
            "a.id = b.id OR a.x = b.x",
            "a.id = (SELECT 1)",
            "(a.id = b.id)",
            "a.id = b.id -- x",
            "a.id = b.id /* x */",
            "a.id = b.\"id\"",
            "a.id = b.`id`",
            "a.id = b.[id]",
            "a.id = 'x'",
            "a.id = 1",
            "a.id = b.1id",
            "a.id = lower(b.id)",
            "id = b.id",
            "a.id = id",
            "a.id = b.id AND",
            "a.id = b.id AND AND a.x = b.x",
            "a.id b.id",
            "a.id == b.id",
            "a.id = b.id a.x = b.x",
            "public.a.id = b.id",
            "a.id = b.id UNION SELECT 1",
            "a.id LIKE b.id",
            "a.id IS NULL",
            "a.id = b.id\0",
            "a.id = b.id AND a.x = $1",
            "a.id = b.id AND a.x = ?",
            "a.id = @p1",
            "a.ïd = b.id",
        ];

        for condition in hostile {
            let error = parse_join_condition(condition, KNOWN)
                .expect_err(&format!("'{condition}' must be rejected"));

            assert!(
                error.to_string().contains("Accepted form"),
                "the rejection of '{condition}' should show the accepted form: {error}"
            );
        }
    }

    #[test]
    fn rejects_a_qualifier_that_is_not_a_table_of_the_query() {
        let error = parse_join_condition("a.id = secrets.id", KNOWN)
            .expect_err("an unknown qualifier must be rejected");

        assert_eq!(
            error,
            JoinConditionError::UnknownQualifier {
                qualifier: "secrets".to_string(),
                known: KNOWN.iter().map(|name| name.to_string()).collect(),
            }
        );
        assert!(error.to_string().contains("known: a, b, users, orders"));
    }

    #[test]
    fn qualifiers_are_matched_exactly() {
        assert!(parse_join_condition("A.id = b.id", KNOWN).is_err());
    }

    #[test]
    fn join_on_conditions_renders_each_side_through_the_callback() {
        let comparisons = parse_join_condition("a.id = b.a_id AND a.k >= b.k", KNOWN)
            .expect("the condition is in the accepted form");

        let on = join_on_conditions(&comparisons, |reference| {
            format!("\"{}\".\"{}\"", reference.qualifier, reference.column)
        });

        let JoinOn::Conditions(JoinFilterNode::Group { op, children, .. }) = on else {
            panic!("expected structured conditions");
        };

        assert_eq!(op, BoolOp::And);
        assert_eq!(children.len(), 2);

        let JoinFilterNode::Predicate(first) = &children[0] else {
            panic!("expected a predicate");
        };
        assert_eq!(first.left, "\"a\".\"id\"");
        assert_eq!(first.op, Comparator::Eq);
        assert_eq!(first.right, "\"b\".\"a_id\"");
    }

    #[test]
    fn plain_identifier_accepts_only_ascii_names() {
        for name in ["users", "_tmp", "order_items2", "A"] {
            assert!(is_plain_sql_identifier(name), "{name}");
        }

        for name in [
            "", "1abc", "a b", "a.b", "a\"b", "a?", "a$1", "a-b", "año", "a;",
        ] {
            assert!(!is_plain_sql_identifier(name), "{name}");
        }
    }
}
