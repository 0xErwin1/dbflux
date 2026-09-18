//! Conservative static pre-flight scan over script source text.
//!
//! This never dispatches anything and never executes the script — it exists
//! solely to decide whether the UI needs one up-front confirmation before
//! running. It is deliberately unsound in the safe direction: it can flag a
//! script that turns out to be harmless (a false positive, costing one extra
//! confirm), but it must never miss a script that turns out to be
//! destructive (a false negative), because the confirmed ceiling this scan
//! produces is what the dispatch-boundary classifier in `engine` enforces
//! against.

/// The outcome of scanning a script's source text before execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaticScanOutcome {
    /// The script contains a construct this engine does not support at all.
    /// `construct` names it (`"await"`, `"require"`, or `"import"`) so the
    /// caller can report a precise error; nothing from the script dispatches.
    Rejected { construct: &'static str },

    /// The scan proves the script cannot reach anything worse than a read —
    /// no write/destructive method name appears anywhere in the source, and
    /// no computed member access (`obj[expr]`) is present that could hide
    /// one. No up-front confirmation is required.
    Read,

    /// The scan cannot prove the script is read-only. Either a write or
    /// worse method name appears in the source (regardless of whether it
    /// sits behind a loop or a conditional), or a computed member access is
    /// present whose runtime value the scan cannot see. One up-front
    /// confirmation is required before the script may run.
    RequiresConfirmation,
}

/// Method names that resolve to a [`dbflux_core::ScriptMethod`] classified
/// above `Read`. Kept in sync with `ScriptMethod::from_js_name` /
/// `ScriptMethod::classification` in `dbflux_core`; a mismatch here is
/// strictly conservative (over-flagging), never under-flagging, since any
/// name absent from this list that turns out to be a write/destructive
/// method would need a matching absence from `ScriptMethod` too, at which
/// point `from_js_name` rejects it and the engine aborts the statement
/// before dispatch regardless of this scan's verdict.
const WRITE_OR_WORSE_METHOD_NAMES: &[&str] = &[
    "insertOne",
    "insertMany",
    "updateOne",
    "updateMany",
    "replaceOne",
    "deleteOne",
    "deleteMany",
    "drop",
    "dropDatabase",
    "createCollection",
    "runCommand",
];

/// Constructs this engine rejects outright, in the order they are checked.
const REJECTED_CONSTRUCTS: &[&str] = &["await", "require", "import"];

/// Scans `source` and returns the conservative pre-flight verdict.
pub fn static_scan(source: &str) -> StaticScanOutcome {
    for construct in REJECTED_CONSTRUCTS {
        if contains_word(source, construct) {
            return StaticScanOutcome::Rejected { construct };
        }
    }

    if reachable_write_or_worse(source) {
        StaticScanOutcome::RequiresConfirmation
    } else {
        StaticScanOutcome::Read
    }
}

fn reachable_write_or_worse(source: &str) -> bool {
    WRITE_OR_WORSE_METHOD_NAMES
        .iter()
        .any(|method| contains_word(source, method))
        || contains_computed_member_access(source)
}

/// True if `source` contains `word` as a whole identifier (not as a
/// substring of a longer identifier, e.g. `require` must not match
/// `requires`).
fn contains_word(source: &str, word: &str) -> bool {
    let bytes = source.as_bytes();
    let word_len = word.len();
    let mut search_from = 0;

    while let Some(relative_pos) = source.get(search_from..).and_then(|s| s.find(word)) {
        let absolute_pos = search_from + relative_pos;

        let before_is_boundary = absolute_pos == 0 || !is_identifier_char(bytes[absolute_pos - 1]);
        let after_index = absolute_pos + word_len;
        let after_is_boundary =
            after_index >= bytes.len() || !is_identifier_char(bytes[after_index]);

        if before_is_boundary && after_is_boundary {
            return true;
        }

        search_from = absolute_pos + 1;
    }

    false
}

/// True if `source` contains an `[` immediately preceded (ignoring
/// whitespace) by an identifier character or `]` — the shape of a computed
/// member access (`db.coll[methodName]()`, `arr[i]`) as opposed to an array
/// literal (`[1, 2, 3]`), which is instead preceded by an operator,
/// delimiter, or nothing.
fn contains_computed_member_access(source: &str) -> bool {
    let bytes = source.as_bytes();

    for (index, &byte) in bytes.iter().enumerate() {
        if byte != b'[' {
            continue;
        }

        let mut lookback = index;
        while lookback > 0 {
            lookback -= 1;
            let candidate = bytes[lookback];
            if (candidate as char).is_whitespace() {
                continue;
            }
            if is_identifier_char(candidate) || candidate == b']' {
                return true;
            }
            break;
        }
    }

    false
}

fn is_identifier_char(byte: u8) -> bool {
    let c = byte as char;
    c.is_alphanumeric() || c == '_' || c == '$'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_level_await_is_rejected_by_name() {
        assert_eq!(
            static_scan("const x = await db.users.find({});"),
            StaticScanOutcome::Rejected { construct: "await" }
        );
    }

    #[test]
    fn require_is_rejected_by_name() {
        assert_eq!(
            static_scan("const fs = require('fs');"),
            StaticScanOutcome::Rejected {
                construct: "require"
            }
        );
    }

    #[test]
    fn import_is_rejected_by_name() {
        assert_eq!(
            static_scan("import fs from 'fs';"),
            StaticScanOutcome::Rejected {
                construct: "import"
            }
        );
    }

    #[test]
    fn require_substring_in_a_longer_identifier_is_not_rejected() {
        // "requires" contains "require" as a substring but is a distinct
        // identifier; the scan must not false-positive on it.
        assert_eq!(
            static_scan("const requiresApproval = true; db.users.find({});"),
            StaticScanOutcome::Read
        );
    }

    #[test]
    fn read_only_script_proves_safe() {
        assert_eq!(
            static_scan("db.users.find({}); db.orders.aggregate([]);"),
            StaticScanOutcome::Read
        );
    }

    #[test]
    fn write_method_anywhere_requires_confirmation() {
        assert_eq!(
            static_scan("db.users.insertOne({name: 'a'});"),
            StaticScanOutcome::RequiresConfirmation
        );
    }

    #[test]
    fn write_method_behind_a_conditional_still_requires_confirmation() {
        assert_eq!(
            static_scan("if (shouldWrite) { db.users.deleteMany({}); }"),
            StaticScanOutcome::RequiresConfirmation
        );
    }

    #[test]
    fn computed_method_name_requires_confirmation() {
        assert_eq!(
            static_scan("const m = 'deleteMany'; db.users[m]({});"),
            StaticScanOutcome::RequiresConfirmation
        );
    }

    #[test]
    fn array_literal_is_not_mistaken_for_computed_access() {
        assert_eq!(
            static_scan("const nums = [1, 2, 3]; db.users.find({id: {$in: nums}});"),
            StaticScanOutcome::Read
        );
    }
}
