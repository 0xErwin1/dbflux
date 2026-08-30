//! Language service for CloudWatch's read-only query surfaces (Logs Insights
//! QL, OpenSearch PPL, and OpenSearch SQL).
//!
//! All three dialects only read log data; none of them expose a query-shaped
//! mutation or delete surface — log group/stream deletion happens through the
//! CloudWatch management API, not through a query string. Without this
//! service, `Connection::language_service()` fell back to
//! `SqlLanguageService`, which ran SQL keyword heuristics and a SQL grammar
//! parser against Insights QL / PPL text. That produced false "dangerous"
//! signals whenever a field happened to be named `delete` or `drop`, and
//! spurious editor syntax-error squiggles on valid Insights QL/PPL pipeline
//! syntax the SQL grammar cannot parse. This service reports honestly instead:
//! never dangerous, never a syntax diagnostic, always a read.

use dbflux_core::{DangerousQueryKind, ExecutionClassification, LanguageService, ValidationResult};

pub struct CloudWatchLanguageService;

impl LanguageService for CloudWatchLanguageService {
    fn validate(&self, _query: &str) -> ValidationResult {
        ValidationResult::Valid
    }

    fn detect_dangerous(&self, _query: &str) -> Option<DangerousQueryKind> {
        None
    }

    fn classify_execution(&self, _query: &str) -> Option<ExecutionClassification> {
        Some(ExecutionClassification::Read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_is_always_permissive() {
        assert!(matches!(
            CloudWatchLanguageService.validate(""),
            ValidationResult::Valid
        ));
        assert!(matches!(
            CloudWatchLanguageService
                .validate("fields @timestamp, @message | filter @message like /error/"),
            ValidationResult::Valid
        ));
    }

    #[test]
    fn delete_as_field_name_is_not_dangerous() {
        // "delete" and "drop" can legitimately appear as log field names in
        // Logs Insights QL / PPL / OpenSearch SQL; the SQL heuristic gate
        // this service replaces must never fire on this text.
        let query = "fields delete, drop | filter delete = \"true\"";
        assert_eq!(CloudWatchLanguageService.detect_dangerous(query), None);
    }

    #[test]
    fn sql_shaped_text_in_a_log_message_is_never_dangerous() {
        // CloudWatch has no query-shaped mutation surface; even text that
        // looks SQL-dangerous stays read-only here.
        let query = "SELECT * FROM logs WHERE message = 'DROP TABLE users'";
        assert_eq!(CloudWatchLanguageService.detect_dangerous(query), None);
    }

    #[test]
    fn ppl_pipeline_is_never_dangerous() {
        let query = "source=my-log-group | where status_code >= 500 | stats count() by service";
        assert_eq!(CloudWatchLanguageService.detect_dangerous(query), None);
    }

    #[test]
    fn classify_execution_is_always_read() {
        assert_eq!(
            CloudWatchLanguageService.classify_execution("fields @timestamp"),
            Some(ExecutionClassification::Read)
        );
        assert_eq!(
            CloudWatchLanguageService.classify_execution(""),
            Some(ExecutionClassification::Read)
        );
    }
}
