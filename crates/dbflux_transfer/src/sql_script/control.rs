use super::ScriptStatement;

/// A statement that starts, ends or checkpoints a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionControl {
    /// 1-based position in the script.
    pub index: usize,
    /// 1-based start line in the file.
    pub line: usize,
    /// The keyword: `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`,
    /// `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `SET autocommit` or
    /// `SET IMPLICIT_TRANSACTIONS`.
    pub keyword: &'static str,
}

/// The first top-level statement that controls transactions. Only a
/// statement that starts with the keyword counts, so a `BEGIN ... END` body
/// inside `CREATE PROCEDURE` or `CREATE TRIGGER` is not one.
///
/// The splitter cuts such a body at its inner `;`, which leaves a lone `END`
/// fragment. A statement that holds a `BEGIN` word opens a compound body, and
/// the next lone `END` closes it instead of counting as a transaction end.
pub(super) fn first_transaction_control(
    statements: &[ScriptStatement],
) -> Option<TransactionControl> {
    let mut open_bodies = 0usize;

    for (position, statement) in statements.iter().enumerate() {
        let keyword = transaction_keyword(&statement.text);

        if keyword == Some("END") && open_bodies > 0 {
            open_bodies -= 1;
            continue;
        }

        if let Some(keyword) = keyword {
            return Some(TransactionControl {
                index: position + 1,
                line: statement.line,
                keyword,
            });
        }

        if statement
            .text
            .split_whitespace()
            .any(|word| word.eq_ignore_ascii_case("begin"))
        {
            open_bodies += 1;
        }
    }

    None
}

/// Words that may follow `BEGIN` or `END` in a transaction statement; any
/// other word after `BEGIN` starts a compound block.
pub(super) const BEGIN_FOLLOWERS: [&str; 9] = [
    "TRANSACTION",
    "TRAN",
    "WORK",
    "DEFERRED",
    "IMMEDIATE",
    "EXCLUSIVE",
    "ISOLATION",
    "READ",
    "NOT",
];

fn transaction_keyword(statement: &str) -> Option<&'static str> {
    let mut words = statement.split_whitespace();
    let first = words.next()?.to_ascii_uppercase();
    let second = words.next().map(str::to_ascii_uppercase);
    let follows_transaction = || {
        second
            .as_deref()
            .is_none_or(|word| BEGIN_FOLLOWERS.contains(&word))
    };

    match first.as_str() {
        "BEGIN" => follows_transaction().then_some("BEGIN"),
        "END" => follows_transaction().then_some("END"),
        "START" => (second.as_deref() == Some("TRANSACTION")).then_some("START TRANSACTION"),
        "COMMIT" => Some("COMMIT"),
        "ROLLBACK" => Some("ROLLBACK"),
        "SAVEPOINT" => Some("SAVEPOINT"),
        "RELEASE" => Some("RELEASE"),
        "SET" => set_keyword(second.as_deref(), words.next()),
        _ => None,
    }
}

/// `SET autocommit ...` and `SET IMPLICIT_TRANSACTIONS ...`, with the
/// `@@`, `SESSION`/`GLOBAL`/`LOCAL` spellings.
fn set_keyword(second: Option<&str>, third: Option<&str>) -> Option<&'static str> {
    let mut variable = second?;
    if matches!(variable, "SESSION" | "GLOBAL" | "LOCAL") {
        variable = third?;
    }

    let name: String = variable
        .trim_start_matches('@')
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    match name.to_ascii_uppercase().as_str() {
        "AUTOCOMMIT" => Some("SET autocommit"),
        "IMPLICIT_TRANSACTIONS" => Some("SET IMPLICIT_TRANSACTIONS"),
        _ => None,
    }
}
