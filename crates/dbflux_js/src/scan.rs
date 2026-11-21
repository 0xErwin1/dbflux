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
    /// no write/destructive method name or `$out`/`$merge` aggregation stage
    /// appears anywhere in the source, and no computed member access (`obj[expr]`) is present that could hide
    /// one. No up-front confirmation is required.
    Read,

    /// The scan cannot prove the script is read-only. Either a write or
    /// worse method name or an output stage appears in the source
    /// (regardless of whether it sits behind a loop or a conditional), or a
    /// computed member access is present whose runtime value the scan cannot
    /// see. One up-front
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
];

/// Method names classified as `AdminDestructive`, kept separate so callers
/// escalate the ceiling above `Destructive` only on a literal mention.
const ADMIN_DESTRUCTIVE_METHOD_NAMES: &[&str] = &["runCommand"];

/// Aggregation stages that write to a collection. `aggregate` itself is a
/// read, so the scan must see the stage to flag the script; the engine
/// classifies the dispatched pipeline the same way (see
/// `engine::classify_operation`).
const OUTPUT_STAGE_NAMES: &[&str] = &["$out", "$merge"];

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

/// True if `source` contains `runCommand` as a bare identifier outside
/// string literals, comments, and regex literals.
///
/// Quoted names (`'runCommand'`, `"runCommand"`) and computed member access
/// (`docs[0]`, `db[m]`) do not match, so ordinary loops and computed calls
/// stay at `Destructive` while [`crate::run`] enforces the confirmed ceiling
/// at dispatch.
pub fn contains_run_command(source: &str) -> bool {
    ADMIN_DESTRUCTIVE_METHOD_NAMES
        .iter()
        .any(|name| contains_identifier(source, name))
}

/// True if `source` contains `target` as a bare JS identifier outside string
/// literals, comments, and regex literals (while still scanning expressions
/// inside `${...}` template interpolations).
fn contains_identifier(source: &str, target: &str) -> bool {
    let bytes = source.as_bytes();
    let mut i = 0;
    let mut template_braces: Vec<usize> = Vec::new();
    let mut regex_allowed = true;

    while let Some(&b) = bytes.get(i) {
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if let Some(next) = skip_comment(bytes, i) {
            i = next;
            continue;
        }
        if b == b'/' && regex_allowed {
            i = skip_regex(bytes, i + 1);
            regex_allowed = false;
            continue;
        }
        if b == b'\'' || b == b'"' {
            i = skip_quoted(bytes, i + 1, b);
            regex_allowed = false;
            continue;
        }
        if b == b'`' {
            (i, regex_allowed) = enter_template_chunk(bytes, i + 1, &mut template_braces);
            continue;
        }
        if b == b'{' {
            if let Some(depth) = template_braces.last_mut() {
                *depth += 1;
            }
            i += 1;
            regex_allowed = true;
            continue;
        }
        if b == b'}' {
            i += 1;
            if let Some(depth) = template_braces.last_mut() {
                *depth -= 1;
                if *depth == 0 {
                    template_braces.pop();
                    (i, regex_allowed) = enter_template_chunk(bytes, i, &mut template_braces);
                    continue;
                }
            }
            regex_allowed = true;
            continue;
        }
        if b.is_ascii_digit() {
            while bytes.get(i).is_some_and(|&c| is_identifier_char(c)) {
                i += 1;
            }
            regex_allowed = false;
            continue;
        }
        if is_identifier_char(b) || b >= 0x80 {
            let start = i;
            while bytes
                .get(i)
                .is_some_and(|&c| is_identifier_char(c) || c >= 0x80)
            {
                i += 1;
            }
            let ident = source.get(start..i).unwrap_or_default();
            if ident == target {
                return true;
            }
            regex_allowed = is_regex_prefix_keyword(ident);
            continue;
        }

        regex_allowed = !matches!(b, b')' | b']' | b'.');
        i += 1;
    }

    false
}

fn skip_comment(bytes: &[u8], i: usize) -> Option<usize> {
    if bytes.get(i) != Some(&b'/') {
        return None;
    }
    if bytes.get(i + 1) == Some(&b'/') {
        let mut cursor = i + 2;
        while bytes.get(cursor).is_some_and(|&c| c != b'\n') {
            cursor += 1;
        }
        return Some(cursor);
    }
    if bytes.get(i + 1) == Some(&b'*') {
        let mut cursor = i + 2;
        while let Some(&c) = bytes.get(cursor) {
            if c == b'*' && bytes.get(cursor + 1) == Some(&b'/') {
                return Some(cursor + 2);
            }
            cursor += 1;
        }
        return Some(cursor);
    }
    None
}

fn enter_template_chunk(
    bytes: &[u8],
    start: usize,
    template_braces: &mut Vec<usize>,
) -> (usize, bool) {
    let (next, entered_expr) = skip_template_chunk(bytes, start);
    if entered_expr {
        template_braces.push(1);
    }
    (next, entered_expr)
}

fn is_regex_prefix_keyword(ident: &str) -> bool {
    matches!(
        ident,
        "return"
            | "case"
            | "throw"
            | "in"
            | "of"
            | "typeof"
            | "void"
            | "delete"
            | "new"
            | "else"
            | "do"
            | "yield"
            | "await"
    )
}

fn skip_quoted(bytes: &[u8], mut i: usize, quote: u8) -> usize {
    while let Some(&c) = bytes.get(i) {
        if c == b'\\' {
            i += 2;
            continue;
        }
        if c == quote || c == b'\n' || c == b'\r' {
            return i + 1;
        }
        i += 1;
    }
    i
}

fn skip_regex(bytes: &[u8], mut i: usize) -> usize {
    let mut in_class = false;
    while let Some(&c) = bytes.get(i) {
        if c == b'\\' {
            if bytes.get(i + 1).is_some_and(|&n| n == b'\n' || n == b'\r') {
                return i + 1;
            }
            i += 2;
            continue;
        }
        if c == b'\n' || c == b'\r' {
            return i + 1;
        }
        if c == b'[' {
            in_class = true;
        } else if c == b']' {
            in_class = false;
        } else if c == b'/' && !in_class {
            i += 1;
            while bytes.get(i).is_some_and(|&f| is_identifier_char(f)) {
                i += 1;
            }
            return i;
        }
        i += 1;
    }
    i
}

fn skip_template_chunk(bytes: &[u8], mut i: usize) -> (usize, bool) {
    while let Some(&c) = bytes.get(i) {
        if c == b'\\' {
            i += 2;
            continue;
        }
        if c == b'`' {
            return (i + 1, false);
        }
        if c == b'$' && bytes.get(i + 1) == Some(&b'{') {
            return (i + 2, true);
        }
        i += 1;
    }
    (i, false)
}

fn reachable_write_or_worse(source: &str) -> bool {
    WRITE_OR_WORSE_METHOD_NAMES
        .iter()
        .chain(ADMIN_DESTRUCTIVE_METHOD_NAMES)
        .chain(OUTPUT_STAGE_NAMES)
        .any(|name| contains_word(source, name))
        || contains_computed_member_access(source)
        || contains_computed_object_key(source)
}

/// True if `source` contains a `]` followed (ignoring whitespace) by `:` —
/// the shape of a computed object key (`{ ["$mer" + "ge"]: "x" }`), which can
/// build a stage name the word search cannot see. A ternary such as
/// `c ? a[0] : b` also matches; over-flagging only costs one confirmation.
fn contains_computed_object_key(source: &str) -> bool {
    source.match_indices(']').any(|(index, _)| {
        source
            .get(index + 1..)
            .is_some_and(|rest| rest.trim_start().starts_with(':'))
    })
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

        #[expect(
            clippy::indexing_slicing,
            reason = "the index evaluates only when the `absolute_pos == 0` short-circuit is false, i.e. 1 <= absolute_pos < bytes.len() because a non-empty word matched at absolute_pos, so absolute_pos - 1 is in bounds"
        )]
        let before_is_boundary = absolute_pos == 0 || !is_identifier_char(bytes[absolute_pos - 1]);
        let after_index = absolute_pos + word_len;
        #[expect(
            clippy::indexing_slicing,
            reason = "the `after_index >= bytes.len()` short-circuit is evaluated first, so the index only runs when after_index < bytes.len()"
        )]
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
            #[expect(
                clippy::indexing_slicing,
                reason = "lookback starts at index < bytes.len() and the `while lookback > 0` guard decrements it, so at the index 0 <= lookback < bytes.len()"
            )]
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
    fn aggregate_output_stage_requires_confirmation() {
        for script in [
            r#"db.orders.aggregate([{ $merge: { into: "archive" } }]);"#,
            r#"db.orders.aggregate([{ "$out": "archive" }]);"#,
            r#"db.orders.aggregate([{ "$facet": { "a": [{ '$merge': "archive" }] } }]);"#,
        ] {
            assert_eq!(
                static_scan(script),
                StaticScanOutcome::RequiresConfirmation,
                "{script}"
            );
        }
    }

    #[test]
    fn computed_object_key_requires_confirmation() {
        for script in [
            r#"db.orders.aggregate([{ ["$mer" + "ge"]: "archive" }]);"#,
            r#"db.orders.aggregate([{ "$match": {}, [key]: "archive" }]);"#,
        ] {
            assert_eq!(
                static_scan(script),
                StaticScanOutcome::RequiresConfirmation,
                "{script}"
            );
        }
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

    #[test]
    fn contains_run_command_detects_literal_run_command() {
        for script in [
            r#"db.runCommand({ ping: 1 });"#,
            r#"const coll = "coll"; db.runCommand({ update: coll, updates: [] });"#,
            r#"const { runCommand } = db; runCommand({ ping: 1 });"#,
            r#"db.users.find({ name: /'/ }); db.runCommand({ ping: 1 });"#,
            r#"const msg = "ends with \\"; db.runCommand({ ping: 1 });"#,
            r#"print(`${db.runCommand({ ping: 1 })}`);"#,
        ] {
            assert!(contains_run_command(script), "{script}");
        }
        assert_eq!(
            static_scan(r#"const coll = "coll"; db.runCommand({ ping: 1 });"#),
            StaticScanOutcome::RequiresConfirmation
        );
    }

    #[test]
    fn contains_run_command_ignores_longer_identifiers() {
        assert!(!contains_run_command(
            "const runCommandHelper = 1; db.users.find({});"
        ));
    }

    #[test]
    fn contains_run_command_ignores_writes_deletes_computed_access_strings_and_comments() {
        for script in [
            "db.users.deleteMany({});",
            "const docs = db.users.find({}); print(docs[0]);",
            "const m = 'run' + 'Command'; db[m]({ ping: 1 });",
            "const m = 'runCommand'; db[m]({ ping: 1 });",
            r#"db["runCommand"]({ ping: 1 });"#,
            "const m = `runCommand`; db[m]({ ping: 1 });",
            "const s = `hello ${{ a: 'runCommand' }.a} runCommand`; db.users.insertOne({ s });",
            r#"const msg = "escaped \" runCommand"; db.users.insertOne({ msg });"#,
            "// runCommand\n/* runCommand */\ndb.users.insertOne({ a: 1 });",
            "db.users.find({ name: /runCommand/i });",
        ] {
            assert!(!contains_run_command(script), "{script}");
        }
    }
}
