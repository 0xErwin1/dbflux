//! SQL script import: a `.sql` file is split into statements and executed
//! against a connection one statement at a time.
//!
//! The engine is driver-agnostic. It lives in `dbflux_transfer` next to the
//! other import engines because it drives a [`dbflux_core::Connection`] and
//! reuses the same transaction vocabulary as the table sink
//! ([`dbflux_core::TransactionVocab`]); nothing here names a driver.
//!
//! The flow a caller follows:
//!
//! 1. [`load_script`] reads and splits the file (size, UTF-8 and emptiness
//!    limits), recording each statement's 1-based start line.
//! 2. [`SqlScript::summarize`] classifies every statement with the
//!    connection's own language service and counts them by
//!    [`dbflux_core::ExecutionClassification`] and
//!    [`dbflux_core::DangerousQueryKind`], so a wizard can show what the
//!    script holds and gate destructive scripts behind a confirmation.
//! 3. [`run_script`] executes the statements with [`RunOptions`], reporting
//!    progress and honoring cancellation, and returns a [`ScriptReport`].
//!
//! Not covered here, and owned by the caller:
//!
//! * Audit. `dbflux_transfer` has no event sink; the caller emits the `Query`
//!   category events (with the connection's audit settings) from the report.
//! * The dangerous-query confirmation settings of the connection. The
//!   summary carries the counts; whether Confirm is shown is a caller policy.
//! * Interrupting a statement already running. [`run_script`] blocks inside
//!   `Connection::execute`, so the caller cancels from another thread by
//!   setting the [`dbflux_core::CancelToken`] and, if it wants the running
//!   statement stopped, calling `Connection::cancel_active`. A statement
//!   that ends with `DbError::Cancelled` is a cancellation, not a failure.

mod load;
mod run;
mod summary;

#[cfg(test)]
mod tests;

pub use load::{MAX_SCRIPT_BYTES, load_script};
pub use run::{
    Disposition, OnError, RunEnd, RunOptions, ScriptProgress, ScriptReport, StatementFailure,
    run_script,
};
pub use summary::{ScriptSummary, StatementInfo};

/// Longest statement snippet carried in summaries, failures and progress.
pub(crate) const SNIPPET_CHARS: usize = 200;

/// One executable statement of a script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptStatement {
    /// The statement text, trimmed, without its terminating `;`.
    pub text: String,
    /// 1-based line of the file where the statement starts.
    pub line: usize,
}

impl ScriptStatement {
    /// The first line of the statement, cut to a short snippet.
    pub fn snippet(&self) -> String {
        let first_line = self.text.lines().next().unwrap_or_default();
        let mut snippet: String = first_line.chars().take(SNIPPET_CHARS).collect();

        if first_line.chars().count() > SNIPPET_CHARS || self.text.lines().nth(1).is_some() {
            snippet.push('…');
        }

        snippet
    }
}

/// A script split into its executable statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlScript {
    statements: Vec<ScriptStatement>,
    bytes: u64,
}

impl SqlScript {
    pub(crate) fn new(statements: Vec<ScriptStatement>, bytes: u64) -> Self {
        Self { statements, bytes }
    }

    /// The statements in file order. Never empty.
    pub fn statements(&self) -> &[ScriptStatement] {
        &self.statements
    }

    pub fn statement_count(&self) -> usize {
        self.statements.len()
    }

    /// Size of the script text in bytes.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Why a script could not be loaded or run. Every variant states what to do
/// next, because the wizard shows the message as is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScriptError {
    #[error(
        "The script is too large: {size} bytes. Scripts over {limit} bytes cannot be run; split the file into smaller scripts and run them one at a time."
    )]
    TooLarge { size: u64, limit: u64 },

    #[error(
        "Cannot read the script: it is not valid UTF-8 (invalid byte at position {position}). SQL scripts are read as UTF-8 and have no encoding option. Save the file as UTF-8 in your editor and choose it again."
    )]
    NotUtf8 {
        /// 1-based byte position of the first invalid byte.
        position: usize,
    },

    #[error(
        "Cannot read the file: {path}: {message} Check that you can read the file, or choose another one."
    )]
    Unreadable { path: String, message: String },

    #[error(
        "The script has no statements: the file is empty or holds only comments. Choose a file that has at least one statement."
    )]
    Empty,

    #[error(
        "Cannot run a SQL script on this connection: it uses the {language} query language, not SQL. Connect to a SQL connection and open the import there, or choose a CSV, JSON, JSON Lines, XML or spreadsheet file to import data into this connection."
    )]
    NotSql { language: String },

    #[error(
        "Running in one transaction is not available together with \"Continue after errors\": a failed statement aborts the transaction on some engines, so the run could not continue after it. Choose \"Stop at the first error\" to run in one transaction."
    )]
    TransactionWithContinue,

    #[error(
        "One transaction is not available: this connection does not support transactions. Each statement runs on its own and is saved as it runs."
    )]
    TransactionUnavailable,

    #[error(
        "This connection is read-only, and {changing} statements in the script change data or structure. Open a read-write connection to run it. A script that only reads can run here."
    )]
    ReadOnlyProfile { changing: usize },

    #[error("Could not start the transaction, so nothing ran: {message}")]
    TransactionStart { message: String },
}
