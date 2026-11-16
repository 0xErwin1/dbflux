use super::ScriptStatement;
use super::blank::{Identifiers, blank_literals, conditional_comment_bodies, words_with_offsets};

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TransactionEffect {
    Opens,
    Closes,
    Savepoint(String),
    Release(String),
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
    statement_words_with(statement, Identifiers::Blank)
}

fn statement_words_with(statement: &str, identifiers: Identifiers) -> Vec<String> {
    words_with_offsets(&blank_literals(statement, identifiers))
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

/// `SET autocommit ...` and `SET IMPLICIT_TRANSACTIONS ...` anywhere in the
/// assignment list (`SET sql_mode = '', autocommit = 1`), with the `@@`,
/// `SESSION`/`GLOBAL`/`LOCAL` spellings.
fn set_keyword(words: &[String]) -> Option<&'static str> {
    words.iter().skip(1).find_map(|word| match word.as_str() {
        "AUTOCOMMIT" => Some("SET autocommit"),
        "IMPLICIT_TRANSACTIONS" => Some("SET IMPLICIT_TRANSACTIONS"),
        _ => None,
    })
}

/// What `statement` does to the session's transaction. `ROLLBACK TO` and a
/// chained end (`COMMIT AND CHAIN`, `ROLLBACK AND CHAIN`, which start the
/// next transaction at once) leave it open.
pub(super) fn transaction_effect(statement: &str) -> TransactionEffect {
    let words = statement_words_with(statement, Identifiers::Keep);
    let name_after = |skip: usize| {
        words
            .iter()
            .skip(skip)
            .find(|word| word.as_str() != "SAVEPOINT")
            .cloned()
            .unwrap_or_default()
    };

    match transaction_keyword(statement) {
        Some("BEGIN" | "START TRANSACTION") => TransactionEffect::Opens,
        Some("COMMIT" | "END" | "ABORT" | "ROLLBACK") => {
            let chained = words
                .windows(2)
                .any(|pair| matches!(pair, [and, chain] if and == "AND" && chain == "CHAIN"));
            let to_savepoint = words.iter().skip(1).take(3).any(|word| word == "TO");

            if chained || to_savepoint {
                TransactionEffect::Other
            } else {
                TransactionEffect::Closes
            }
        }
        Some("SAVEPOINT") => TransactionEffect::Savepoint(name_after(1)),
        Some("RELEASE") => TransactionEffect::Release(name_after(1)),
        _ => TransactionEffect::Other,
    }
}

/// Follows the transaction a script opens on its own with `BEGIN` or `START
/// TRANSACTION`. A `SAVEPOINT` outside one is only noted: whether it opens a
/// transaction depends on the engine, and the connection cannot be asked.
/// Chained ends (`COMMIT AND CHAIN`) keep the transaction open.
#[derive(Debug, Default)]
pub(super) struct TransactionTracker {
    open: bool,
    savepoint_outside_transaction: bool,
}

impl TransactionTracker {
    pub(super) fn apply(&mut self, statement: &str) {
        match transaction_effect(statement) {
            TransactionEffect::Opens => self.open = true,
            TransactionEffect::Closes => *self = Self::default(),
            TransactionEffect::Savepoint(_) => {
                if !self.open {
                    self.savepoint_outside_transaction = true;
                }
            }
            TransactionEffect::Release(_) | TransactionEffect::Other => {}
        }
    }

    pub(super) fn used_savepoint_outside_transaction(&self) -> bool {
        self.savepoint_outside_transaction
    }

    pub(super) fn is_open(&self) -> bool {
        self.open
    }
}

/// The line of the first version-conditional comment whose body controls
/// transactions or switches the session to manual commit. Whether such a
/// comment runs depends on the server and its version, so the script cannot
/// be followed.
pub(super) fn conditional_transaction_control_line(
    statements: &[ScriptStatement],
) -> Option<usize> {
    statements.iter().find_map(|statement| {
        conditional_comment_bodies(&statement.text)
            .into_iter()
            .find(|(_, body)| {
                body.split(';').any(|piece| {
                    transaction_keyword(piece).is_some()
                        || session_commit_mode_change(piece).is_some()
                })
            })
            .map(|(offset, _)| statement.line + offset)
    })
}

/// What a statement does when it switches the session to manual commit
/// (`SET autocommit = 0/OFF/FALSE`, `SET IMPLICIT_TRANSACTIONS ON`), or
/// `None`. Every assignment of a `SET` list is checked, quoted values count,
/// and the body of a `/*!...*/` conditional comment is read as code. Turning
/// autocommit on, or implicit transactions off, is fine.
pub(super) fn session_commit_mode_change(statement: &str) -> Option<&'static str> {
    let words = statement_words_with(statement, Identifiers::KeepStrings);
    if words.first().map(String::as_str) != Some("SET") {
        return None;
    }

    words
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(position, variable)| {
            let value = words
                .iter()
                .skip(position + 1)
                .find(|word| word.as_str() != "TO")?;

            match (variable.as_str(), value.as_str()) {
                ("AUTOCOMMIT", "0" | "OFF" | "FALSE") => Some("turns off autocommit"),
                ("IMPLICIT_TRANSACTIONS", "ON") => Some("turns on implicit transactions"),
                _ => None,
            }
        })
}
