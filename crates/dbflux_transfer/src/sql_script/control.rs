use super::ScriptStatement;
use super::blank::{Identifiers, blank_literals, words_with_offsets};

/// A statement that starts, ends or checkpoints a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionControl {
    /// 1-based position in the script.
    pub index: usize,
    /// 1-based start line in the file.
    pub line: usize,
    /// The keyword: `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ABORT`,
    /// `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `SET autocommit` or
    /// `SET IMPLICIT_TRANSACTIONS`.
    pub keyword: &'static str,
}

/// What a statement does to the session's transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransactionEffect {
    Opens,
    Closes,
    Other,
}

/// The first top-level statement that controls transactions. Only a
/// statement that starts with the keyword counts, so a `BEGIN ... END` body
/// inside `CREATE PROCEDURE` or `CREATE TRIGGER` is not one (a script that
/// holds a body cut by the splitter is refused when it is loaded). Comments,
/// strings and quoted identifiers are blanked first.
pub(super) fn first_transaction_control(
    statements: &[ScriptStatement],
) -> Option<TransactionControl> {
    statements
        .iter()
        .enumerate()
        .find_map(|(position, statement)| {
            transaction_keyword(&statement.text).map(|keyword| TransactionControl {
                index: position + 1,
                line: statement.line,
                keyword,
            })
        })
}

/// Words that may follow `BEGIN` or `END` in a transaction statement.
const TRANSACTION_FOLLOWERS: [&str; 9] = [
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

/// Whether a `BEGIN` followed by `next` and `after` starts a transaction.
/// `BEGIN NOT ATOMIC` opens a compound body; any other word after `BEGIN`
/// does too.
pub(super) fn begin_is_transaction(next: Option<&str>, after: Option<&str>) -> bool {
    if next == Some("NOT") && after == Some("ATOMIC") {
        return false;
    }

    next.is_none_or(|word| TRANSACTION_FOLLOWERS.contains(&word))
}

fn statement_words(statement: &str) -> Vec<String> {
    words_with_offsets(&blank_literals(statement, Identifiers::Blank))
        .into_iter()
        .map(|(_, word)| word)
        .collect()
}

pub(super) fn transaction_keyword(statement: &str) -> Option<&'static str> {
    let words = statement_words(statement);
    let word = |position: usize| words.get(position).map(String::as_str);

    match word(0)? {
        "BEGIN" => begin_is_transaction(word(1), word(2)).then_some("BEGIN"),
        "END" => word(1)
            .is_none_or(|next| TRANSACTION_FOLLOWERS.contains(&next))
            .then_some("END"),
        "START" => (word(1) == Some("TRANSACTION")).then_some("START TRANSACTION"),
        "COMMIT" => Some("COMMIT"),
        "ABORT" => Some("ABORT"),
        "ROLLBACK" => Some("ROLLBACK"),
        "SAVEPOINT" => Some("SAVEPOINT"),
        "RELEASE" => Some("RELEASE"),
        "SET" => set_keyword(&words),
        _ => None,
    }
}

/// `SET autocommit ...` and `SET IMPLICIT_TRANSACTIONS ...`, with the
/// `@@`, `SESSION`/`GLOBAL`/`LOCAL` spellings.
fn set_keyword(words: &[String]) -> Option<&'static str> {
    let variable = words
        .iter()
        .skip(1)
        .find(|word| !matches!(word.as_str(), "SESSION" | "GLOBAL" | "LOCAL" | "PERSIST"))?;

    match variable.as_str() {
        "AUTOCOMMIT" => Some("SET autocommit"),
        "IMPLICIT_TRANSACTIONS" => Some("SET IMPLICIT_TRANSACTIONS"),
        _ => None,
    }
}

/// Whether `statement` opens or closes a transaction of the session
/// (`ROLLBACK TO savepoint` does neither).
pub(super) fn transaction_effect(statement: &str) -> TransactionEffect {
    match transaction_keyword(statement) {
        Some("BEGIN" | "START TRANSACTION") => TransactionEffect::Opens,
        Some("COMMIT" | "END" | "ABORT") => TransactionEffect::Closes,
        Some("ROLLBACK") => {
            let words = statement_words(statement);
            if words.iter().skip(1).take(3).any(|word| word == "TO") {
                TransactionEffect::Other
            } else {
                TransactionEffect::Closes
            }
        }
        _ => TransactionEffect::Other,
    }
}

/// What a statement does when it switches the session to manual commit
/// (`SET autocommit = 0/OFF/FALSE`, `SET IMPLICIT_TRANSACTIONS ON`), or
/// `None`. Turning autocommit on, or implicit transactions off, is fine.
pub(super) fn session_commit_mode_change(statement: &str) -> Option<&'static str> {
    let words = statement_words(statement);
    if words.first().map(String::as_str) != Some("SET") {
        return None;
    }

    let mut rest = words
        .iter()
        .skip(1)
        .map(String::as_str)
        .skip_while(|word| matches!(*word, "SESSION" | "GLOBAL" | "LOCAL" | "PERSIST"));
    let variable = rest.next()?;
    let value = rest.find(|word| *word != "TO")?;

    match (variable, value) {
        ("AUTOCOMMIT", "0" | "OFF" | "FALSE") => Some("turns off autocommit"),
        ("IMPLICIT_TRANSACTIONS", "ON") => Some("turns on implicit transactions"),
        _ => None,
    }
}
