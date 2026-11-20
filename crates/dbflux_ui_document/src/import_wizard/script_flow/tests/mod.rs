mod audit;
mod configure;
mod interrupt;
mod keyboard;
mod messages;
mod options;
mod pick;
mod render;
mod run;
mod session;
mod stale;
mod support;

use dbflux_core::ExecutionClassification;
use dbflux_transfer::sql_script::ScriptSummary;

/// A summary with `total` statements of which `changing` change data.
pub(super) fn summary_with(total: usize, changing: usize) -> ScriptSummary {
    ScriptSummary {
        statement_count: total,
        bytes: 0,
        by_classification: vec![(ExecutionClassification::Write, changing)],
        by_danger: Vec::new(),
        destructive_count: 0,
        changing_count: changing,
        ddl_count: 0,
        ddl_commits_on_its_own: false,
        transaction_control: None,
        preview: Vec::new(),
        first_destructive: Vec::new(),
    }
}
