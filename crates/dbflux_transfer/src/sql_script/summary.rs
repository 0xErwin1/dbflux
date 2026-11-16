use dbflux_core::{
    Connection, DangerousQueryKind, ExecutionClassification,
    classify_query_for_language_with_service,
};

use super::routine::changes_a_sequence;
use super::{SqlScript, TransactionControl};

/// Order in which [`ScriptSummary::by_classification`] lists the classes.
const CLASSIFICATION_ORDER: [ExecutionClassification; 7] = [
    ExecutionClassification::Metadata,
    ExecutionClassification::Read,
    ExecutionClassification::Write,
    ExecutionClassification::Destructive,
    ExecutionClassification::AdminSafe,
    ExecutionClassification::Admin,
    ExecutionClassification::AdminDestructive,
];

/// One statement as the summary reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementInfo {
    /// 1-based position in the script.
    pub index: usize,
    /// 1-based start line in the file.
    pub line: usize,
    pub snippet: String,
    pub classification: ExecutionClassification,
    /// The dangerous pattern the connection's language service finds in this
    /// statement on its own (`DELETE` without `WHERE`, `DROP`, ...).
    pub danger: Option<DangerousQueryKind>,
}

impl StatementInfo {
    /// Destructive: the statement is classified destructive or matches a
    /// dangerous pattern.
    pub fn is_destructive(&self) -> bool {
        self.danger.is_some() || is_destructive_class(self.classification)
    }
}

/// What a script holds, computed before anything runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptSummary {
    pub statement_count: usize,
    pub bytes: u64,
    /// Statement counts per classification, in a fixed order, classes with no
    /// statement left out.
    pub by_classification: Vec<(ExecutionClassification, usize)>,
    /// Statement counts per dangerous pattern, in order of first appearance.
    pub by_danger: Vec<(DangerousQueryKind, usize)>,
    /// Statements that are destructive (see [`StatementInfo::is_destructive`]).
    pub destructive_count: usize,
    /// Statements that are not a plain read of data or metadata.
    pub changing_count: usize,
    /// Schema statements (`AdminSafe`, `Admin`, `AdminDestructive`).
    pub ddl_count: usize,
    /// The script has schema statements and the connection commits them on
    /// its own, so a rollback cannot undo them
    /// (`Connection::supports_transactional_ddl` is false).
    pub ddl_commits_on_its_own: bool,
    /// The first statement that controls transactions itself. A script with
    /// one cannot run in one transaction (see
    /// [`ScriptError::SelfManagedTransaction`](super::ScriptError)).
    pub transaction_control: Option<TransactionControl>,
    /// The first statements of the script, for the preview.
    pub preview: Vec<StatementInfo>,
    /// The first destructive statements, in script order.
    pub first_destructive: Vec<StatementInfo>,
}

impl SqlScript {
    /// Classifies every statement on its own with the connection's language
    /// and language service (the editor's classification), then counts them.
    /// `limit` bounds `preview` and `first_destructive`.
    pub fn summarize(&self, connection: &dyn Connection, limit: usize) -> ScriptSummary {
        let language_service = connection.language_service();
        let classes = classify_all(self, connection);

        let mut counts = [0usize; CLASSIFICATION_ORDER.len()];
        let mut by_danger: Vec<(DangerousQueryKind, usize)> = Vec::new();
        let mut preview = Vec::new();
        let mut first_destructive = Vec::new();
        let mut destructive_count = 0;

        for (position, (statement, classification)) in
            self.statements().iter().zip(&classes).enumerate()
        {
            if let Some(count) = CLASSIFICATION_ORDER
                .iter()
                .position(|class| class == classification)
                .and_then(|slot| counts.get_mut(slot))
            {
                *count += 1;
            }

            let danger = language_service.detect_dangerous(&statement.text);
            if let Some(kind) = danger {
                match by_danger.iter_mut().find(|(found, _)| *found == kind) {
                    Some((_, count)) => *count += 1,
                    None => by_danger.push((kind, 1)),
                }
            }

            let info = StatementInfo {
                index: position + 1,
                line: statement.line,
                snippet: statement.snippet(),
                classification: *classification,
                danger,
            };

            if info.is_destructive() {
                destructive_count += 1;
                if first_destructive.len() < limit {
                    first_destructive.push(info.clone());
                }
            }
            if preview.len() < limit {
                preview.push(info);
            }
        }

        let by_classification: Vec<_> = CLASSIFICATION_ORDER
            .iter()
            .copied()
            .zip(counts)
            .filter(|(_, count)| *count > 0)
            .collect();
        let ddl_count = by_classification
            .iter()
            .filter(|(class, _)| is_ddl_class(*class))
            .map(|(_, count)| count)
            .sum();
        let changing_count = classes.iter().filter(|class| changes_data(**class)).count();

        ScriptSummary {
            statement_count: self.statement_count(),
            bytes: self.bytes(),
            by_classification,
            by_danger,
            destructive_count,
            changing_count,
            ddl_count,
            ddl_commits_on_its_own: ddl_count > 0 && !connection.supports_transactional_ddl(),
            transaction_control: self.transaction_control(),
            preview,
            first_destructive,
        }
    }
}

/// The classification of each statement, in script order, from the
/// connection's own language and language service.
pub(super) fn classify_all(
    script: &SqlScript,
    connection: &dyn Connection,
) -> Vec<ExecutionClassification> {
    let language = &connection.metadata().query_language;
    let service = connection.language_service();

    script
        .statements()
        .iter()
        .map(|statement| {
            let classification =
                classify_query_for_language_with_service(language, &statement.text, Some(service));

            if changes_data(classification) || !changes_a_sequence(&statement.text) {
                classification
            } else {
                ExecutionClassification::Write
            }
        })
        .collect()
}

pub(super) fn changes_data(classification: ExecutionClassification) -> bool {
    !matches!(
        classification,
        ExecutionClassification::Metadata | ExecutionClassification::Read
    )
}

pub(super) fn is_ddl_class(classification: ExecutionClassification) -> bool {
    matches!(
        classification,
        ExecutionClassification::AdminSafe
            | ExecutionClassification::Admin
            | ExecutionClassification::AdminDestructive
    )
}

fn is_destructive_class(classification: ExecutionClassification) -> bool {
    matches!(
        classification,
        ExecutionClassification::Destructive | ExecutionClassification::AdminDestructive
    )
}
