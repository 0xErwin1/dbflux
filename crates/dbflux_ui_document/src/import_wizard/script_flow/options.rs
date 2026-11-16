//! Pure rules of the "Run SQL script" flow: what the options mean for the
//! connection, which of them are on offer, and how counts and sizes are
//! written. No GPUI.

use std::time::Duration;

use dbflux_core::{Connection, DriverCapabilities, TransactionVocab};
use dbflux_transfer::sql_script::{OnError, RunOptions, ScriptSummary};

/// What the connection allows the options to be, fixed when the script is
/// read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::import_wizard) struct ConnectionRules {
    /// The connection declares transactions and has a begin, commit and
    /// rollback vocabulary.
    pub(in crate::import_wizard) transactions_supported: bool,
    /// The profile is read-only by its own setting.
    pub(in crate::import_wizard) read_only_profile: bool,
}

impl ConnectionRules {
    pub(in crate::import_wizard) fn of(
        connection: &dyn Connection,
        read_only_profile: bool,
    ) -> Self {
        Self {
            transactions_supported: connection.supports(DriverCapabilities::TRANSACTIONS)
                && TransactionVocab::for_kind(connection.kind()).is_some(),
            read_only_profile,
        }
    }
}

/// The state of the "Run in one transaction" option.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::import_wizard) enum TransactionAvailability {
    /// The user can turn it on or off.
    Available,
    /// Off because "Continue after errors" is chosen: a failed statement
    /// aborts the transaction on some engines.
    OffForContinue,
    /// The connection has no transactions.
    Unavailable,
}

/// What the user chose on Configure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::import_wizard) struct ScriptOptions {
    pub(in crate::import_wizard) on_error: OnError,
    /// The checkbox as the user left it. It is kept while "Continue after
    /// errors" turns the option off, so going back restores it.
    pub(in crate::import_wizard) transaction: bool,
}

impl Default for ScriptOptions {
    /// The safe defaults: stop at the first error, one transaction.
    fn default() -> Self {
        Self {
            on_error: OnError::Stop,
            transaction: true,
        }
    }
}

impl ScriptOptions {
    pub(in crate::import_wizard) fn availability(
        &self,
        rules: &ConnectionRules,
    ) -> TransactionAvailability {
        if !rules.transactions_supported {
            TransactionAvailability::Unavailable
        } else if self.on_error == OnError::Continue {
            TransactionAvailability::OffForContinue
        } else {
            TransactionAvailability::Available
        }
    }

    /// Whether the run uses one transaction.
    pub(in crate::import_wizard) fn runs_in_transaction(&self, rules: &ConnectionRules) -> bool {
        self.transaction && self.availability(rules) == TransactionAvailability::Available
    }

    pub(in crate::import_wizard) fn run_options(&self, rules: &ConnectionRules) -> RunOptions {
        RunOptions {
            on_error: self.on_error,
            transaction: self.runs_in_transaction(rules),
            read_only_profile: rules.read_only_profile,
        }
    }
}

/// The number of statements that change data or structure when the profile
/// is read-only, which refuses the script; `None` when it can run.
pub(in crate::import_wizard) fn read_only_conflict(
    summary: &ScriptSummary,
    rules: &ConnectionRules,
) -> Option<usize> {
    (rules.read_only_profile && summary.changing_count > 0).then_some(summary.changing_count)
}

/// A size in bytes as `4.2 MB`, `240 MB` or `812 B`.
pub(in crate::import_wizard) fn size_label(bytes: u64) -> String {
    const KILOBYTE: f64 = 1024.0;
    const MEGABYTE: f64 = KILOBYTE * 1024.0;

    let size = bytes as f64;
    if size < KILOBYTE {
        format!("{bytes} B")
    } else if size < MEGABYTE {
        format!("{:.1} KB", size / KILOBYTE)
    } else if size < MEGABYTE * 10.0 {
        format!("{:.1} MB", size / MEGABYTE)
    } else {
        format!("{:.0} MB", size / MEGABYTE)
    }
}

/// `value` with `separator` between groups of three digits.
pub(in crate::import_wizard) fn group_digits(value: usize, separator: &str) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3 * separator.len());

    for (position, digit) in digits.chars().enumerate() {
        if position > 0 && (digits.len() - position).is_multiple_of(3) {
            grouped.push_str(separator);
        }
        grouped.push(digit);
    }

    grouped
}

/// `value` grouped with the locale's thousands separator.
pub(in crate::import_wizard) fn grouped(value: usize) -> String {
    group_digits(
        value,
        &dbflux_i18n::t!("document.import_wizard.script.thousands_separator"),
    )
}

/// Seconds with one decimal: `8.4 s`.
pub(in crate::import_wizard) fn elapsed_label(elapsed: Duration) -> String {
    format!("{:.1} s", elapsed.as_secs_f64())
}

/// A catalog text that counts statements: `<base>.one` when `count` is 1,
/// else `<base>.many`, with `%{count}` grouped and every `extra` pair
/// replaced.
pub(in crate::import_wizard) fn counted(
    base: &str,
    count: usize,
    extra: &[(&str, String)],
) -> String {
    let form = if count == 1 { "one" } else { "many" };
    let mut text = dbflux_i18n::translate(&format!("{base}.{form}"));
    text = text.replace("%{count}", &grouped(count));

    for (name, value) in extra {
        text = text.replace(&format!("%{{{name}}}"), value);
    }

    text
}
